import asyncio 
import uvicorn 
from src.file_handler import FileHandler
from src.utils.logger import setup_logger
from src.api import app, process_submission
from src.utils.config_manager import ConfigManager

config = ConfigManager()
logger = setup_logger()
    
upload_directory = config.get('Monitor','DIR')
file_handler = FileHandler(upload_directory, process_submission)

def start_file_monitor():
    file_handler.start()

async def run_file_monitor():
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, start_file_monitor)

async def serve_api():
    cornfig = uvicorn.Config(f"src.api:{app}", host="127.0.0.1", port=8000, log_level="info")
    server = uvicorn.Server(cornfig)
    await server.serve()

async def main():
    task1 = asyncio.create_task(serve_api()) 
    task2 = asyncio.create_task(run_file_monitor())
    await asyncio.gather(task1, task2)


if __name__ == "__main__":
    asyncio.run(main())