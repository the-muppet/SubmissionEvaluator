import asyncio
import uvicorn
from core.file_handler import FileHandler
from utils.logger import setup_logger
from utils.config_manager import ConfigManager
from core.api import process_submission

config = ConfigManager()
logger = setup_logger()

upload_directory = config.get("Monitor", "DIR")
file_handler = FileHandler(upload_directory, process_submission)

shutdown_event = asyncio.Event()


def start_file_monitor():
    file_handler.start()


async def run_file_monitor():
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, start_file_monitor)
    await shutdown_event.wait()
    file_handler.stop()


async def serve_api():
    config = uvicorn.Config(
        "core.api:app", host="127.0.0.1", port=8000, log_level="debug", reload=True
    )
    server = uvicorn.Server(config)
    await server.serve()
    await shutdown_event.wait()


async def main():
    tasks = [
        asyncio.create_task(serve_api()),
        asyncio.create_task(run_file_monitor()),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        shutdown_event.set()
        # Cancel all tasks
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
