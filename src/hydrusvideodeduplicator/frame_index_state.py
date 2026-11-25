"""Disposable exact-search completion and acknowledged-submission cache."""
from __future__ import annotations

from contextlib import closing
from pathlib import Path
import sqlite3

STATE_FILENAME = '-frame-search-state.sqlite'
STATE_VERSION = '2'
ALGORITHM = 'exact_hamming_lt31_directional_score_v1'


class FrameStateError(RuntimeError):
    pass


def state_path_for(database: Path) -> Path:
    database = Path(database)
    return database.with_name(database.stem + STATE_FILENAME)


class FrameSearchState:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        try:
            tables = {row[0] for row in self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not tables:
                self.connection.executescript('''
                    BEGIN IMMEDIATE;
                    CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                    CREATE TABLE inventory(file_hash TEXT PRIMARY KEY,digest BLOB NOT NULL);
                    CREATE TABLE completed(context TEXT,file_hash TEXT,digest BLOB,min_score INTEGER,
                                           PRIMARY KEY(context,file_hash));
                    CREATE TABLE submitted(context TEXT,hash_a TEXT,hash_b TEXT,digest_a BLOB,digest_b BLOB,
                                           PRIMARY KEY(context,hash_a,hash_b));
                    CREATE TABLE pending(context TEXT,hash_a TEXT,hash_b TEXT,digest_a BLOB,digest_b BLOB,
                                         score_a_to_b REAL,score_b_to_a REAL,
                                         PRIMARY KEY(context,hash_a,hash_b));
                ''')
                self.connection.executemany('INSERT INTO metadata VALUES (?,?)',
                                             [('version',STATE_VERSION),('algorithm',ALGORITHM)])
                self.connection.commit()
            if tables == {'metadata','inventory','completed','submitted'}:
                old_meta = dict(self.connection.execute('SELECT key,value FROM metadata'))
                if old_meta == {'version':'1','algorithm':ALGORITHM}:
                    with self.connection:
                        self.connection.execute('''CREATE TABLE pending(
                            context TEXT,hash_a TEXT,hash_b TEXT,digest_a BLOB,digest_b BLOB,
                            score_a_to_b REAL,score_b_to_a REAL,
                            PRIMARY KEY(context,hash_a,hash_b))''')
                        self.connection.execute("UPDATE metadata SET value=? WHERE key='version'", (STATE_VERSION,))
                    tables.add('pending')
            if tables and tables != {'metadata','inventory','completed','submitted','pending'}:
                raise FrameStateError('Unexpected exact-search state tables; retain the file for inspection.')
            meta = dict(self.connection.execute('SELECT key,value FROM metadata'))
            if meta != {'version':STATE_VERSION,'algorithm':ALGORITHM}:
                raise FrameStateError('Exact-search state version differs; retain the file for inspection.')
        except BaseException:
            self.connection.close()
            raise

    def close(self):
        self.connection.close()

    def prepare(self, current):
        """Preserve completions on deletion-only changes, reset on additions/replacements."""
        previous = dict(self.connection.execute('SELECT file_hash,digest FROM inventory'))
        expanded = any(previous.get(key) != value for key,value in current.items())
        with self.connection:
            if expanded:
                self.connection.execute('DELETE FROM completed')
            self.connection.execute('DELETE FROM inventory')
            self.connection.executemany('INSERT INTO inventory VALUES (?,?)', current.items())
            self.connection.execute('''DELETE FROM completed WHERE NOT EXISTS (
                SELECT 1 FROM inventory i WHERE i.file_hash=completed.file_hash AND i.digest=completed.digest)''')
            self.connection.execute('''DELETE FROM submitted WHERE NOT EXISTS (
                SELECT 1 FROM inventory i WHERE i.file_hash=submitted.hash_a AND i.digest=submitted.digest_a)
                OR NOT EXISTS (SELECT 1 FROM inventory i WHERE i.file_hash=submitted.hash_b AND i.digest=submitted.digest_b)''')
            self.connection.execute('''DELETE FROM pending WHERE NOT EXISTS (
                SELECT 1 FROM inventory i WHERE i.file_hash=pending.hash_a AND i.digest=pending.digest_a)
                OR NOT EXISTS (SELECT 1 FROM inventory i WHERE i.file_hash=pending.hash_b AND i.digest=pending.digest_b)''')
        return expanded

    def completed_at(self, context, cutoff):
        return {key for key, in self.connection.execute(
            'SELECT file_hash FROM completed WHERE context=? AND min_score<=?', (context,cutoff))}

    def finish_query(self, context, file_hashes, digests, cutoff):
        self.finish_queries(context, [file_hashes], digests, cutoff)

    def finish_queries(self, context, query_file_hashes, digests, cutoff):
        """Checkpoint several completed collections in one transaction."""
        rows = ((context, file_hash, digests[file_hash], cutoff)
                for file_hashes in query_file_hashes for file_hash in file_hashes)
        with self.connection:
            self.connection.executemany('''INSERT INTO completed VALUES (?,?,?,?)
                ON CONFLICT(context,file_hash) DO UPDATE SET digest=excluded.digest,
                min_score=MIN(completed.min_score,excluded.min_score)''',
                rows)

    def already_submitted(self, context, hash_a, hash_b, digests):
        left,right = sorted((hash_a,hash_b))
        return self.connection.execute('''SELECT 1 FROM submitted WHERE context=? AND hash_a=? AND hash_b=?
            AND digest_a=? AND digest_b=?''', (context,left,right,digests[left],digests[right])).fetchone() is not None

    def acknowledge(self, context, hash_a, hash_b, digests):
        left,right = sorted((hash_a,hash_b))
        with self.connection:
            self.connection.execute('INSERT OR REPLACE INTO submitted VALUES (?,?,?,?,?)',
                                    (context,left,right,digests[left],digests[right]))
            self.connection.execute('DELETE FROM pending WHERE context=? AND hash_a=? AND hash_b=?',
                                    (context,left,right))

    def record_pending(self, context, hash_a, hash_b, digests, score):
        left,right = sorted((hash_a,hash_b))
        column = 'score_a_to_b' if hash_a == left else 'score_b_to_a'
        with self.connection:
            self.connection.execute('''INSERT OR IGNORE INTO pending
                (context,hash_a,hash_b,digest_a,digest_b) VALUES (?,?,?,?,?)''',
                (context,left,right,digests[left],digests[right]))
            self.connection.execute(f'UPDATE pending SET {column}=? WHERE context=? AND hash_a=? AND hash_b=?',
                                    (float(score),context,left,right))

    def pending_pairs(self, context):
        return self.connection.execute('''SELECT hash_a,hash_b,score_a_to_b,score_b_to_a
            FROM pending WHERE context=? ORDER BY hash_a,hash_b''', (context,)).fetchall()


def reset_frame_search_state(path: Path):
    if not path.exists():
        return
    with closing(FrameSearchState(path)) as state, state.connection:
        state.connection.execute('DELETE FROM completed')
        state.connection.execute('DELETE FROM submitted')
        state.connection.execute('DELETE FROM pending')
