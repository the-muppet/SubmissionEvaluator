from datetime import datetime
from pathlib import Path
import re
import shutil
from src.models import UploadFile
from fastapi import FastAPI, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from src.utils.logger import setup_logger
from src.utils.config_manager import ConfigManager

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
async def upload_file(
    email: str = Form(...), storeName: str = Form(...), file: UploadFile = File(...)
):
    try:
        sanitized_name = sanitize_for_path(storeName)
        email_sanitized = sanitize_for_path(email)
        date_str = datetime.now().strftime("%Y%m%d%H%M%S")

        uploads_dir = Path(__file__).parent / "uploads" / sanitized_name
        uploads_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{email_sanitized}_{date_str}.csv"
        file_path = uploads_dir / filename

        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    return JSONResponse(content={
        "status": "Success",
        "value": 100,  # Example value
        "quantity": 5,  # Example quantity
        "acv": 200  # Example ACV
    })
