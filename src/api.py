import re
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict
from src.models import Submission
from src.utils.logger import setup_logger
from src.data_loader import DataLoader
from fastapi.staticfiles import StaticFiles
from src.utils.config_manager import ConfigManager
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from src.evaluator import SubmissionEvaluator
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    UploadFile,
    BackgroundTasks,
    WebSocket,
)

# Create the FastAPI app
app = FastAPI()

# Assuming setup_logger() and ConfigManager() are defined elsewhere
logger = setup_logger()
config = ConfigManager()

BASE_DIR = Path(__file__).resolve().parent.parent
static_files_path = BASE_DIR / "static"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Specify the correct origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=static_files_path), name="static")


@app.get("/")
async def get_form():
    logger.info("serving main page")
    return FileResponse("static/index.html")


@app.get("/syp")
async def get_syp():
    logger.info("Serving syp page")
    return FileResponse("static/syp.html")


def sanitize_for_path(name: str) -> str:
    """
    Sanitizes the name for use in path by replacing invalid characters underscores.
    """
    sanitized_name = re.sub(r"[\/\\:*?\"<>|]", "_", name)
    logger.debug(f"Sanitized {sanitized_name}.")
    return sanitized_name


def process_email(email: str) -> str:
    """
    Processes the email to remove the '.com' part and any domain-specific extensions.
    """
    email_without_domain = email.split("@")[0]  # Removes domain
    logger.debug(f"Processed email {email_without_domain}")
    return email_without_domain


def process_submission(file_path=None, store_name=None, email=None):
    try:
        logger.info(f"Processing submission for store {store_name} and email {email}")
        submission_df = DataLoader(config).read_csv_file(file_path)
        submission = Submission.new_submission(submission_df, store_name, email)
        eval_frame = EvalFrame(submission, DataLoader(config))
        evaluation_df = eval_frame.evaluation_df
        evaluator = Evaluator(evaluation_df)
        results = evaluator.evaluate()
        logger.info("Successfully processed submission")
        return results
    except Exception as e:
        logger.error(f"Failed to process submission: {e}")
        return None


async def process_submission_background(
    file_path: Path, store_name: str, email: str, client_id: str
):
    logger.info(f"Initiating background processing for client_id: {client_id}")
    results = process_submission(
        file_path=file_path, store_name=store_name, email=email
    )
    if results is None:
        logger.warning(f"Processing failed for client_id: {client_id}")
        await send_message_to_client(client_id, "Processing failed")
    else:
        logger.info(
            f"Processing successful for client_id: {client_id}, sending results."
        )
        await send_message_to_client(client_id, results)


@app.post("/submit/")
async def upload_file(
    background_tasks: BackgroundTasks,
    email: str = Form(...),
    storeName: str = Form(...),
    file: UploadFile = File(...),
    client_id: str = Form(...),
):
    try:
        logger.info(f"Received file upload request from {email} for store {storeName}.")
        sanitized_name = sanitize_for_path(storeName)
        email_sanitized = sanitize_for_path(email)
        date_str = datetime.now().strftime("%Y%m%d%H%M%S")

        uploads_dir = Path("uploads") / sanitized_name
        uploads_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{email_sanitized}_{date_str}.csv"
        file_path = uploads_dir / filename

        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        logger.info(
            f"File {filename} uploaded successfully. Initiating background processing."
        )

        background_tasks.add_task(
            process_submission_background, file_path, storeName, email, client_id
        )

    except Exception as e:
        logger.error(f"Error processing file upload: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    return {"message": "File uploaded, file processing initiated"}


connections: Dict[str, WebSocket] = {}


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await connect_client(client_id, websocket)
    try:
        while True:
            # Wait for any message from the client
            _ = await websocket.receive_text()
            # No specific action taken on message receipt for now
            # The connection remains open for sending messages back to the client
    except Exception as e:
        logger.error(f"WebSocket error with {client_id}: {e}")
    finally:
        await disconnect_client(client_id)


async def connect_client(client_id: str, websocket: WebSocket):
    await websocket.accept()
    connections[client_id] = websocket
    logger.info(f"WebSocket connection established with client_id: {client_id}")
    await websocket.send_text(f"Connected to server with client ID: {client_id}")


async def disconnect_client(client_id: str):
    if client_id in connections:
        await connections[client_id].close()
        del connections[client_id]
        logger.info(f"WebSocket connection closed for client_id: {client_id}")


async def send_message_to_client(client_id: str, message: str):
    if client_id in connections:
        await connections[client_id].send_text(message)
        logger.debug(f"Sent message to client_id: {client_id}")


async def process_submission_background(
    file_path: Path, store_name: str, email: str, client_id: str
):
    try:
        logger.debug("Background submission statement entered")
        results = process_submission(
            file_path=file_path, store_name=store_name, email=email
        )
        if results is None:
            await send_message_to_client(client_id, "Processing failed")
        else:
            # Assuming `results` is already a JSON serializable dict
            message = json.dumps(results)
            await send_message_to_client(client_id, message)
    except Exception as e:
        logger.error(f"Error processing submission for {client_id}: {e}")
        await send_message_to_client(client_id, "Error during processing")
