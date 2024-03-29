from pathlib import Path
from utils.logger import setup_logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logger = setup_logger()


class FileHandler(FileSystemEventHandler):
    def __init__(self, directory, callback):
        self.directory = str(Path(directory).resolve())
        self.callback = callback
        self.observer = Observer()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            logger.info(f"Detected new file: {event.src_path}")
            self.callback(file_path=event.src_path)

    def start(self):
        logger.info(f"Starting to monitor directory: {self.directory}")
        self.observer.schedule(self, self.directory, recursive=False)
        self.observer.start()

        try:
            self.observer.join()
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("Stopped monitoring directory.")
        self.observer.join()
