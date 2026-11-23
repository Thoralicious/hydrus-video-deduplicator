"""Out-of-process resource sampler: remains schedulable while native code holds the GIL."""
from __future__ import annotations
import argparse
import ctypes
from datetime import datetime, timezone
import json
import mmap
import os
import signal
import sys
from pathlib import Path
import struct
import time

HEADER = struct.Struct('<II')
STATUS_SIZE = 16384


class ProcessSampler:
    def __init__(self, pid):
        self.pid = pid
        self.handle = None
        if os.name == 'nt':
            from ctypes import wintypes as w
            class Memory(ctypes.Structure):
                _fields_ = [('cb',w.DWORD),('PageFaultCount',w.DWORD)]+[(name,ctypes.c_size_t) for name in (
                    'PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage','QuotaPagedPoolUsage',
                    'QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage','PagefileUsage','PeakPagefileUsage','PrivateUsage')]
            class IO(ctypes.Structure):
                _fields_ = [(name,ctypes.c_ulonglong) for name in ('ReadOperationCount','WriteOperationCount',
                    'OtherOperationCount','ReadTransferCount','WriteTransferCount','OtherTransferCount')]
            self.Memory,self.IO,self.w = Memory,IO,w
            self.kernel = ctypes.WinDLL('kernel32',use_last_error=True)
            self.psapi = ctypes.WinDLL('psapi',use_last_error=True)
            self.kernel.OpenProcess.argtypes = [w.DWORD,w.BOOL,w.DWORD]
            self.kernel.OpenProcess.restype = w.HANDLE
            self.kernel.CloseHandle.argtypes = [w.HANDLE]
            self.kernel.GetExitCodeProcess.argtypes = [w.HANDLE,ctypes.POINTER(w.DWORD)]
            self.kernel.GetProcessTimes.argtypes = [w.HANDLE]+[ctypes.POINTER(w.FILETIME)]*4
            self.kernel.GetProcessIoCounters.argtypes = [w.HANDLE,ctypes.POINTER(IO)]
            self.psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE,ctypes.POINTER(Memory),w.DWORD]
            self.handle = self.kernel.OpenProcess(0x0400|0x0010,False,pid)
            if not self.handle: raise OSError(ctypes.get_last_error(),'Cannot inspect monitored process')

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None

    def sample(self):
        if os.name == 'nt':
            exit_code = self.w.DWORD()
            if not self.kernel.GetExitCodeProcess(self.handle,ctypes.byref(exit_code)) or exit_code.value != 259:
                return None
            memory = self.Memory(); memory.cb = ctypes.sizeof(memory)
            if not self.psapi.GetProcessMemoryInfo(self.handle,ctypes.byref(memory),memory.cb):
                raise OSError(ctypes.get_last_error(),'Cannot sample memory')
            times = [self.w.FILETIME() for _ in range(4)]
            if not self.kernel.GetProcessTimes(self.handle,*(ctypes.byref(value) for value in times)):
                raise OSError(ctypes.get_last_error(),'Cannot sample CPU')
            ticks = lambda value: (value.dwHighDateTime<<32)|value.dwLowDateTime
            result = {'cpu_seconds':(ticks(times[2])+ticks(times[3]))/10000000,
                      'rss_bytes':memory.WorkingSetSize,'os_peak_rss_bytes':memory.PeakWorkingSetSize,
                      'private_bytes':memory.PrivateUsage,'os_peak_commit_bytes':memory.PeakPagefileUsage}
            io = self.IO()
            if self.kernel.GetProcessIoCounters(self.handle,ctypes.byref(io)):
                result.update(read_transfer_bytes=io.ReadTransferCount,write_transfer_bytes=io.WriteTransferCount)
            return result
        proc = Path('/proc')/str(self.pid)
        try:
            stat = (proc/'stat').read_text().rsplit(')',1)[1].split()
            if stat[0] == 'Z': return None
            result = {'cpu_seconds':(int(stat[11])+int(stat[12]))/os.sysconf('SC_CLK_TCK')}
            fields = {}
            for line in (proc/'status').read_text().splitlines():
                key,_,value = line.partition(':')
                fields[key] = value.strip()
            for source,target in (('VmRSS','rss_bytes'),('VmHWM','os_peak_rss_bytes')):
                if source in fields: result[target] = int(fields[source].split()[0])*1024
            try:
                io = dict(line.split(':',1) for line in (proc/'io').read_text().splitlines())
                result.update(read_transfer_bytes=int(io['rchar']),write_transfer_bytes=int(io['wchar']))
            except (OSError,KeyError,ValueError): pass
            return result
        except (FileNotFoundError,ProcessLookupError):
            return None


def read_status(shared, previous):
    first = bytes(shared[:HEADER.size])
    sequence,length = HEADER.unpack(first)
    if sequence%2 or not 0 < length <= STATUS_SIZE-HEADER.size: return previous
    payload = bytes(shared[HEADER.size:HEADER.size+length])
    if first != bytes(shared[:HEADER.size]): return previous
    try: return json.loads(payload)
    except (ValueError,UnicodeError): return previous


def monitor(pid, status_path, output_path, interval, terminal=True):
    sampler = ProcessSampler(pid)
    previous = None
    status = {}
    peaks = {}
    samples = 0
    errors = 0
    try:
        with open(status_path,'r+b') as status_file, mmap.mmap(status_file.fileno(),STATUS_SIZE) as shared, \
                open(output_path,'x',encoding='utf-8') as output:
            while True:
                status = read_status(shared,status)
                now = time.monotonic()
                try:
                    resources = sampler.sample()
                    if resources is None: break
                    resources['cpu_percent_one_core'] = None if previous is None else max(0,
                        100*(resources['cpu_seconds']-previous[1])/(now-previous[0]))
                    previous = (now,resources['cpu_seconds'])
                    for name in ('rss_bytes','private_bytes','os_peak_rss_bytes','os_peak_commit_bytes'):
                        if name in resources: peaks[name] = max(peaks.get(name,0),resources[name])
                    samples += 1
                    age = max(0,now-status.get('last_progress_at',now))
                    record = {'event':'resource_sample','timestamp_utc':datetime.now(timezone.utc).isoformat(),
                              'monotonic_seconds':now,'pid':pid,'phase':status.get('phase','run'),
                              'stage':status.get('stage','starting'),
                              'stage_elapsed_seconds':max(0,now-status.get('stage_started',now)),
                              'seconds_since_observed_progress':age,'progress':status.get('progress',{}),
                              'details':status.get('details',{}),'current_phase_has_counter':status.get('current_phase_has_counter',False),**resources}
                    output.write(json.dumps(record)+'\n'); output.flush()
                    if terminal and not status.get('closed'):
                        cpu = resources['cpu_percent_one_core']
                        cpu_text = '?' if cpu is None else f'{cpu:.0f}%'
                        rss = resources.get('rss_bytes',0)/1024**2
                        progress = record['progress']
                        detail = ', '.join(f'{key}={value}' for key,value in list(progress.items())[:5])
                        if not detail: detail = 'no internal progress counter for this phase'
                        elif not record['current_phase_has_counter']: detail += '; current phase has no internal counter'
                        print(f"[monitor] {record['stage']}: {record['stage_elapsed_seconds']:.1f}s; {detail}; "
                              f"last observed progress {age:.1f}s ago; CPU {cpu_text} (100%=one core); RSS {rss:.0f} MiB",flush=True)
                except (OSError,ValueError,KeyError) as error:
                    errors += 1
                    output.write(json.dumps({'event':'monitor_error','error_type':type(error).__name__})+'\n'); output.flush()
                if status.get('closed'): break
                deadline = time.monotonic()+interval
                while time.monotonic()<deadline:
                    time.sleep(min(.1,max(0,deadline-time.monotonic())))
                    status = read_status(shared,status)
                    if status.get('closed'): break
    finally:
        sampler.close()
        summary = {'samples':samples,'sampling_errors':errors,'peaks':peaks,
                   'scope':'Monitored HVD process and its threads; watchdog/other processes excluded.',
                   'cpu_units':'100 percent equals one logical CPU; values can exceed 100.',
                   'io_units':'Cumulative process transfer counters, not physical disk throughput.',
                   'peak_note':'Sampled RSS/private peaks can miss short spikes; os_peak fields cover process lifetime.'}
        Path(str(output_path)+'.summary.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')


def main(argv=None):
    signal.signal(signal.SIGINT,signal.SIG_IGN)
    parser = argparse.ArgumentParser()
    parser.add_argument('--pid',type=int,required=True)
    parser.add_argument('--status',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--interval',type=float,default=2)
    args = parser.parse_args(argv)
    monitor(args.pid,args.status,args.output,max(.5,args.interval))


def run_if_requested():
    if len(sys.argv) > 1 and sys.argv[1] == '--hvd-internal-diagnostic-watchdog':
        main(sys.argv[2:])
        return True
    return False


if __name__ == '__main__':
    main()
