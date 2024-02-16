import re
import json
import aiofiles
from pathlib import Path
from datetime import datetime
from typing import Dict
from utils.handshake import (
    generate_salt,
    hash_email_with_salt,
    read_salts_from_file,
    write_salt_to_file,
)
from models.my_validators import EmailInput
from models.submission_class import ClientIdResponse, Submission
from utils.logger import setup_logger
from .data_loader import DataLoader
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from utils.config_manager import ConfigManager
from fastapi.middleware.cors import CORSMiddleware
from core.evaluator import Evaluator
from fastapi.templating import Jinja2Templates
from fastapi import (
    FastAPI,
    Request,
    WebSocket,
    Form,
    File,
    UploadFile,
)


# Create the FastAPI app
app = FastAPI()

# import logging and config
logger = setup_logger()
config = ConfigManager()
templates = Jinja2Templates(directory="static/templates")

# Enable CORS for all origins"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Path resolution
BASE_DIR = Path(__file__).resolve().parent.parent
upload_files_path = BASE_DIR / "uploads"

# Ensure the uploads directory exists
upload_files_path.mkdir(parents=True, exist_ok=True)


@app.post("/get-client-id/", response_model=ClientIdResponse)
async def get_client_id(email_input: EmailInput):
    email = email_input.email
    salts = read_salts_from_file()
    # Check if the client/email is already salty, shake it on em if not
    if email in salts:
        salt = bytes.fromhex(salts[email])
    else:
        salt = generate_salt()
        write_salt_to_file(email, salt)

    # Generate the client_id using the email and salt
    client_id = hash_email_with_salt(email, salt)

    # Return the generated client_id using ClientIdResponse model
    return ClientIdResponse(client_id=client_id)


@app.get("/")
async def get_syp(request: Request):
    return templates.TemplateResponse("syp.html", {"request": request})


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
        eval_frame = Evaluator(submission, DataLoader(config))
        evaluation_df = eval_frame.evaluation_df
        evaluator = Evaluator(evaluation_df)
        results = evaluator.evaluate()
        logger.info("Successfully processed submission")
        return results
    except Exception as e:
        logger.error(f"Failed to process submission: {e}")
        return None


async def send_message_to_client(client_id: str, message: str):
    """
    Sends a message to the client with the given client_id.
    """
    await manager.send_message(message, manager.get_websocket(client_id))


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
    email: str = Form(...), storeName: str = Form(...), file: UploadFile = File(...)
):
    try:
        # Construct filename with sanitized store name and current timestamp
        sanitized_name = re.sub(r"[\/\\:*?\"<>|]", "_", storeName)
        filename = f"{sanitized_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

        file_location = f"uploads/{filename}"
        async with aiofiles.open(file_location, "wb") as out_file:
            content = await file.read()  # Read file content
            await out_file.write(content)  # Save file

        # Here, you can trigger background processing or additional logic as needed
        return {"message": "File uploaded successfully", "filename": filename}

    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})


### WEBSOCKETS ###
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, client_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]

    async def send_personal_message(self, client_id: str, message: str):
        websocket = self.active_connections.get(client_id)
        if websocket:
            await websocket.send_text(message)


manager = ConnectionManager()


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(client_id, websocket)
    try:
        while True:
            # Waiting for any message from the client
            await websocket.receive_text()
    except Exception as e:
        manager.disconnect(client_id)
