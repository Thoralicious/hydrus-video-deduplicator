#pragma once
#include <pdq/cpp/common/pdqhashtypes.h>
#include <array>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>
#include <thread>
#include <exception>
#include <memory>

namespace hvd_native {
// Exact for the existing strict Hamming <31 predicate only.
// With sixteen disjoint 16-bit blocks, <=30 errors imply some block has <=1.
class ExactFrameIndex {
    struct Frame { alignas(8) std::array<char,32> bytes; std::uint32_t owner; };
    std::vector<std::uint32_t> bucket_sizes_ =
        std::vector<std::uint32_t>(16*65536, 0);
    std::vector<std::uint64_t> bucket_offsets_;
    std::vector<std::uint32_t> postings_;
    std::vector<Frame> frames_;
    std::vector<std::uint64_t> ids_;
    std::unordered_set<std::uint64_t> used_ids_;
    std::vector<std::uint32_t> seen_frames_,seen_owners_,counts_;
    struct Scratch {
        std::vector<std::uint32_t> frames,owners,counts;
    };
    std::vector<Scratch> worker_scratch_;
    std::atomic<std::uint64_t> workers_active_{0},scratch_bytes_{0};
    mutable std::mutex mutex_;
    bool sealed_=false,failed_=false;
    std::atomic<bool> active_{false},cancelled_{false};
    std::atomic<std::uint64_t> total_{0},done_{0},checks_{0},visits_{0};
    static std::uint16_t block(const char* bytes,std::size_t part) {
        return static_cast<unsigned char>(bytes[part*2]) |
            (static_cast<std::uint16_t>(static_cast<unsigned char>(bytes[part*2+1]))<<8);
    }
public:
    ExactFrameIndex()=default;
    ExactFrameIndex(const ExactFrameIndex&)=delete;
    void add(std::uint64_t id,std::string_view packed) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (sealed_ || failed_) throw std::runtime_error("index is sealed or failed");
        if (packed.size()%32) throw std::invalid_argument("packed hashes must be multiples of 32 bytes");
        if (used_ids_.count(id)) throw std::invalid_argument("collection ID already indexed");
        if (packed.size()/32>std::numeric_limits<std::uint32_t>::max()-frames_.size() ||
            ids_.size()>=std::numeric_limits<std::uint32_t>::max()) throw std::length_error("32-bit frame/collection limit exceeded");
        try {
            auto owner=static_cast<std::uint32_t>(ids_.size());
            ids_.push_back(id);used_ids_.insert(id);
            for (std::size_t offset=0;offset<packed.size();offset+=32) {
                Frame frame{};std::memcpy(frame.bytes.data(),packed.data()+offset,32);frame.owner=owner;
                frames_.push_back(frame);
                for (std::size_t part=0;part<16;++part)
                    ++bucket_sizes_[part*65536+block(frame.bytes.data(),part)];
            }
        } catch (...) { failed_=true;throw; }
    }
    void seal() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (failed_) throw std::runtime_error("index failed during construction");
        if (sealed_) return;
        try {
            bucket_offsets_.resize(bucket_sizes_.size()+1);
            std::uint64_t posting_count=0;
            for (std::size_t bucket=0;bucket<bucket_sizes_.size();++bucket) {
                bucket_offsets_[bucket]=posting_count;
                posting_count+=bucket_sizes_[bucket];
            }
            bucket_offsets_.back()=posting_count;
            if (posting_count>postings_.max_size())
                throw std::length_error("posting array exceeds addressable size");

            postings_.resize(static_cast<std::size_t>(posting_count));
            std::fill(bucket_sizes_.begin(),bucket_sizes_.end(),0);

            // Preserve the original ascending frame order within each bucket.
            for (std::size_t frame_id=0;frame_id<frames_.size();++frame_id) {
                const auto& frame=frames_[frame_id];
                for (std::size_t part=0;part<16;++part) {
                    const auto bucket=part*65536+block(frame.bytes.data(),part);
                    const auto position=bucket_offsets_[bucket]+bucket_sizes_[bucket]++;
                    postings_[static_cast<std::size_t>(position)]=
                        static_cast<std::uint32_t>(frame_id);
                }
            }
            std::vector<std::uint32_t>().swap(bucket_sizes_);
            seen_frames_.resize(frames_.size());
            seen_owners_.resize(ids_.size());
            counts_.resize(ids_.size());
            sealed_=true;
        } catch (...) {
            failed_=true;
            throw;
        }
    }
    std::vector<std::pair<std::uint64_t,int>> search(std::string_view query,int threshold=50,int workers=1) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!sealed_ || failed_) throw std::runtime_error("seal a successfully built index first");
        if (query.size()%32) throw std::invalid_argument("query hashes must be multiples of 32 bytes");
        if (threshold<1 || threshold>100) throw std::invalid_argument("score threshold must be 1..100");
        if (workers<1 || workers>256) throw std::invalid_argument("workers must be 1..256");
        std::uint64_t count=query.size()/32;
        if (count>=std::numeric_limits<std::uint32_t>::max()) throw std::length_error("query too large");
        cancelled_.store(false);total_.store(count);done_.store(0);checks_.store(0);visits_.store(0);active_.store(true);
        struct Guard { std::atomic<bool>& flag;~Guard(){flag.store(false);} } guard{active_};
        // One shared immutable index; each worker owns its deduplication stamps and counts.
        auto actual=static_cast<std::size_t>(std::min<std::uint64_t>(workers,(count+15)/16));
        if (count>0) {
            // Each task claims 16 query frames. Keep at least four tasks' worth
            // of work per worker so thread startup and per-worker scratch costs
            // do not overwhelm short searches. A nonempty query still uses one
            // worker, even when it has fewer than four tasks.
            constexpr std::uint64_t min_frames_per_worker=64;
            const auto work_limited_workers=std::max<std::uint64_t>(1,count/min_frames_per_worker);
            actual=static_cast<std::size_t>(std::min<std::uint64_t>(actual,work_limited_workers));
        }
        workers_active_.store(actual);
        struct WorkerGuard { std::atomic<std::uint64_t>& n; ~WorkerGuard(){n.store(0);} } worker_guard{workers_active_};
        if (worker_scratch_.size()<actual) worker_scratch_.resize(actual);
        // Allocate before launching threads so allocation failure cannot leave live threads.
        for (std::size_t i=0;i<actual;++i) {
            auto& scratch=worker_scratch_[i];
            if (i==0) {
                std::fill(seen_frames_.begin(),seen_frames_.end(),0);
                std::fill(seen_owners_.begin(),seen_owners_.end(),0);
            } else {
                scratch.frames.assign(frames_.size(),0);
                scratch.owners.assign(ids_.size(),0);
            }
            scratch.counts.assign(ids_.size(),0);
        }
        std::uint64_t scratch_capacity=0;
        for (const auto& scratch:worker_scratch_)
            scratch_capacity+=(scratch.frames.capacity()+scratch.owners.capacity()+scratch.counts.capacity())*sizeof(std::uint32_t);
        scratch_bytes_.store(scratch_capacity);
        std::atomic<std::uint64_t> next{0};
        std::exception_ptr error;
        std::mutex error_mutex;
        auto work=[&](std::size_t worker) {
            auto& seen_frames=worker==0 ? seen_frames_ : worker_scratch_[worker].frames;
            auto& seen_owners=worker==0 ? seen_owners_ : worker_scratch_[worker].owners;
            auto& counts=worker_scratch_[worker].counts;
            try {
                for (;;) {
                    const auto first=next.fetch_add(16);
                    if (first>=count || cancelled_.load()) break;
                    const auto last=std::min<std::uint64_t>(first+16,count);
                    for (auto qi=first;qi<last;++qi) {
                        const auto stamp=qi+1;const char* hash=query.data()+static_cast<std::size_t>(qi)*32;
                        std::uint64_t local_checks=0,local_visits=0;
                        for (std::size_t part=0;part<16;++part) {
                            auto value=block(hash,part);
                            for (int flip=-1;flip<16;++flip) {
                                if (cancelled_.load()) throw std::runtime_error("frame-index search cancelled");
                                auto key=static_cast<std::uint16_t>(value^(flip<0?0:(1u<<flip)));
                                const auto bucket=part*65536+key;
                                for (auto position=bucket_offsets_[bucket];
                                position<bucket_offsets_[bucket+1];++position) {
                                    const auto frame_id=postings_[static_cast<std::size_t>(position)];
                                    ++local_visits;
                                    if ((local_visits&4095)==0 && cancelled_.load()) throw std::runtime_error("frame-index search cancelled");
                                    if (seen_frames[frame_id]==stamp) continue;
                                    seen_frames[frame_id]=stamp;
                                    const auto& frame=frames_[frame_id];
                                    if (seen_owners[frame.owner]==stamp) continue; // one match suffices for this query frame
                                    ++local_checks;
                                    if (facebook::pdq::hashing::hammingDistanceSpanLessThan(
                                            hash, frame.bytes.data(), 31)) {
                                        seen_owners[frame.owner]=stamp;++counts[frame.owner];
                                    }
                                }
                            }
                        }
                        visits_.fetch_add(local_visits);checks_.fetch_add(local_checks);done_.fetch_add(1);
                    }
                }

            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                if (!error) error=std::current_exception();
                cancelled_.store(true);
            }
        };
        std::vector<std::thread> threads;
        try {
            threads.reserve(actual ? actual-1 : 0);
            for (std::size_t i=1;i<actual;++i) threads.emplace_back(work,i);
            if (actual) work(0);
        } catch (...) {
            cancelled_.store(true);
            for (auto& thread:threads) if (thread.joinable()) thread.join();
            throw;
        }
        for (auto& thread:threads) thread.join();
        if (error) std::rethrow_exception(error);
        std::fill(counts_.begin(),counts_.end(),0);
        for (std::size_t i=0;i<actual;++i)
            for (std::size_t owner=0;owner<ids_.size();++owner)
                counts_[owner]+=worker_scratch_[i].counts[owner];
        if (cancelled_.load()) throw std::runtime_error("frame-index search cancelled");
        std::vector<std::pair<std::uint64_t,int>> result;
        if (count) for (std::size_t owner=0;owner<ids_.size();++owner) {
            int score=static_cast<int>((counts_[owner]*100.0)/count);
            if (score>=threshold) result.emplace_back(ids_[owner],score);
        }
        return result;
    }
    std::array<std::uint64_t,2> parallel_stats() const {
        return {workers_active_.load(),scratch_bytes_.load()};
    }
    void release_search_scratch() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<Scratch>().swap(worker_scratch_);
        scratch_bytes_.store(0);
    }
    bool cancel() { if (!active_.load()) return false;cancelled_.store(true);return true; }
    std::array<std::uint64_t,5> progress() const {
        return {active_.load()?1u:0u,total_.load(),done_.load(),checks_.load(),visits_.load()};
    }
    std::map<std::string,std::uint64_t> storage_stats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint64_t padding=
            sizeof(Frame)-sizeof(std::array<char,32>)-sizeof(std::uint32_t);
        std::map<std::string,std::uint64_t> values{
            {"frame_count",frames_.size()},
            {"frame_capacity",frames_.capacity()},
            {"frame_size_bytes",sizeof(Frame)},
            {"frame_padding_bytes_per_frame",padding},
            {"frame_hash_payload_used_bytes",frames_.size()*32},
            {"frame_owner_used_bytes",frames_.size()*sizeof(std::uint32_t)},
            {"frame_padding_used_bytes",frames_.size()*padding},
            {"frames_used_bytes",frames_.size()*sizeof(Frame)},
            {"frames_capacity_bytes",frames_.capacity()*sizeof(Frame)},
            {"frames_spare_capacity_bytes",
                (frames_.capacity()-frames_.size())*sizeof(Frame)},
            {"collection_count",ids_.size()},
            {"collection_id_capacity",ids_.capacity()},
            {"collection_ids_capacity_bytes",
                ids_.capacity()*sizeof(std::uint64_t)},
            {"bucket_counts_capacity_bytes",
                bucket_sizes_.capacity()*sizeof(std::uint32_t)},
            {"bucket_offsets_capacity_bytes",
                bucket_offsets_.capacity()*sizeof(std::uint64_t)},
            {"posting_count",postings_.size()},
            {"posting_capacity",postings_.capacity()},
            {"postings_used_bytes",postings_.size()*sizeof(std::uint32_t)},
            {"postings_capacity_bytes",
                postings_.capacity()*sizeof(std::uint32_t)},
            {"postings_spare_capacity_bytes",
                (postings_.capacity()-postings_.size())*sizeof(std::uint32_t)},
            {"seen_frames_capacity_bytes",
                seen_frames_.capacity()*sizeof(std::uint32_t)},
            {"seen_owners_capacity_bytes",
                seen_owners_.capacity()*sizeof(std::uint32_t)},
            {"score_counts_capacity_bytes",
                counts_.capacity()*sizeof(std::uint32_t)},
            {"used_id_set_entries",used_ids_.size()},
            {"used_id_set_bucket_count",used_ids_.bucket_count()},
            {"parallel_scratch_capacity_bytes",scratch_bytes_.load()},
            {"index_object_bytes",sizeof(*this)}
        };
        values["native_capacity_bytes"]=
            values.at("frames_capacity_bytes")+
            values.at("collection_ids_capacity_bytes")+
            values.at("bucket_counts_capacity_bytes")+
            values.at("bucket_offsets_capacity_bytes")+
            values.at("postings_capacity_bytes")+
            values.at("seen_frames_capacity_bytes")+
            values.at("seen_owners_capacity_bytes")+
            values.at("score_counts_capacity_bytes")+scratch_bytes_.load();
        return values;
    }
    std::array<std::uint64_t,3> stats() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::uint64_t bytes=bucket_sizes_.capacity()*sizeof(std::uint32_t)+
            bucket_offsets_.capacity()*sizeof(std::uint64_t)+
            postings_.capacity()*sizeof(std::uint32_t)+frames_.capacity()*sizeof(Frame)+
            ids_.capacity()*sizeof(std::uint64_t)+
            (seen_frames_.capacity()+seen_owners_.capacity()+counts_.capacity())*sizeof(std::uint32_t);
        // Hash-set allocator overhead is not included; this is capacity accounting, not RSS.
        return {frames_.size(),ids_.size(),bytes+scratch_bytes_.load()};
    }
};
}
