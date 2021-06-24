import sys
import time
import logging
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import LoggingEventHandler, PatternMatchingEventHandler

# had to do:
# sudo sysctl fs.inotify.max_user_watches=100000


class TomlCreatedEventHandler(PatternMatchingEventHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(patterns=["*.toml"])

    def on_created(self, event):
        print(f"Toml file {event.src_path} created")
        with open(event.src_path, "r") as fh:
            print(fh.readline())

    def on_modified(self, event):
        print(f"Toml file {event.src_path} modified")
        with open(event.src_path, "r") as fh:
            print(fh.readline())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    path = sys.argv[1] if len(sys.argv) > 1 else "."
    event_handler = LoggingEventHandler()

    toml_handler = TomlCreatedEventHandler()

    # We use the polling observer as inotify
    # does not see remote file creation events
    observer = Observer(polling_interval=60)
    observer.schedule(event_handler, path, recursive=True)
    observer.schedule(toml_handler, path, recursive=True)
    print("Starting observer")
    observer.start()
    print("Observer started")
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
