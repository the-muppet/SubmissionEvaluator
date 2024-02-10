from datetime import datetime
from pathlib import Path
import base64
import re
import shutil
from src.models import UploadFile
from fastapi import FastAPI, File, Form, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from src.utils.logger import setup_logger
from src.utils.config_manager import ConfigManager

app = FastAPI()

# Assuming setup_logger() and ConfigManager() are defined elsewhere
logger = setup_logger()
config = ConfigManager()

BASE_DIR = Path(__file__).resolve().parent.parent
static_files_path = BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=static_files_path), name="static")


@app.get("/")
async def get_form():
    return FileResponse("static/index.html")

@app.get("/syp")
async def get_syp():
    return FileResponse("static/syp.html")



def sanitize_for_path(name: str) -> str:
    """
    Sanitizes the name for use in path by replacing invalid characters underscores.
    """
    return re.sub(r"[\/\\:*?\"<>|]", "_", name)


def process_email(email: str) -> str:
    """
    Processes the email to remove the '.com' part and any domain-specific extensions.
    """
    email_without_domain = email.split("@")[0]  # Removes domain
    return email_without_domain


@app.post("/submit/")
async def upload_file(upload_data: UploadFile):
    try:
        # Process and sanitize storeName and email
        storeName_sanitized = sanitize_for_path(upload_data.storeName)
        email_processed = process_email(upload_data.email)
        email_sanitized = sanitize_for_path(email_processed)
        date_str = datetime.now().strftime("%Y%m%d%H%M%S")

        base64_file_content = upload_data.file.split(",")[1]
        file_bytes = base64.b64decode(base64_file_content)

        # Creating a directory for the storeName within the uploads folder
        uploads_dir = Path(__file__).parent / "uploads"
        store_dir = uploads_dir / storeName_sanitized
        store_dir.mkdir(parents=True, exist_ok=True)

        # Constructing filename with processed email and date
        filename = f"{email_sanitized}_{date_str}.csv"
        file_path = store_dir / filename

        with file_path.open("wb") as buffer:
            buffer.write(file_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    return {
        "detail": f"File uploaded successfully to {storeName_sanitized} folder with filename: {filename}"
    }

@app.post("/submitForm/")
async def upload_file(storeName: str = Form(...), email: str = Form(...), file: UploadFile = File(...)):
    try:
        storeName_sanitized = sanitize_for_path(storeName)  # Assume implementation exists
        email_sanitized = sanitize_for_path(email)  # Assume implementation exists
        date_str = datetime.now().strftime("%Y%m%d%H%M%S")

        uploads_dir = Path(__file__).parent / "uploads" / storeName_sanitized
        uploads_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{email_sanitized}_{date_str}.csv"
        file_path = uploads_dir / filename

        # Save uploaded file to disk
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    return {
        "detail": f"File uploaded successfully to {storeName_sanitized} folder with filename: {filename}"
    }