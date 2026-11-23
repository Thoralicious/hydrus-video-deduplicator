"""Cross-platform advisory lock shared by this version's CLI operations."""

from contextlib import contextmanager
from pathlib import Path
import os


class DatabaseBusyError(RuntimeError):
    pass


@contextmanager
def database_lock(database: Path):
    database.parent.mkdir(parents=True, exist_ok=True)
    # Keep this inode after release; unlinking creates a race with waiting processes.
    with (database.parent / ".hvd-operation.lock").open("a+b") as stream:
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise DatabaseBusyError("Another CLI operation is using this database folder.") from error
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
