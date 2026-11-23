from hydrusvideodeduplicator.entrypoint import run_main
from hydrusvideodeduplicator.diagnostic_watchdog import run_if_requested

if __name__ == "__main__":
    if not run_if_requested():
        run_main(gui=False, is_executable=True)
