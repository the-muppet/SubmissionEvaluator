from utils.logger import setup_logger
from watchdog.observers import Observer
from utils.config_manager import ConfigManager
from watchdog.events import FileSystemEventHandler

logger = setup_logger()
config = ConfigManager()


class FileHandler(FileSystemEventHandler):
    def __init__(self, directory, callback):
        self.directory = directory
        self.callback = callback
        self.observer = Observer()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            logger.info(f"Detected new file: {event.src_path}")
            self.callback(file_path=event.src_path)

    def start(self):
        self.observer.schedule(self, self.directory, recursive=False)
        self.observer.start()
        logger.info(f"Started monitoring directory: {self.directory}")
        try:
            self.observer.join()
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("Stopped monitoring directory.")
            self.observer.join()
