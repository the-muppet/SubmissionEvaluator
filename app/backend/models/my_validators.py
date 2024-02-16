from pydantic import BaseModel, EmailStr, validator
from datetime import datetime
from fastapi import UploadFile

# Validators


@validator("email")
class EmailInput(BaseModel):
    email: EmailStr

    def __init__(self, email):
        self.email = email.lower()


@validator("storeName")
class StoreNameInput(BaseModel):
    storeName: str

    def __init__(self, storeName):
        self.storeName = storeName.lower()


@validator("date_time")
class DateTimeInput(BaseModel):
    date_time: datetime

    def __init__(self, date_time):
        self.date_time = date_time.replace(tzinfo=None)


@validator("file")
class FileInput(BaseModel):
    file: UploadFile

    def __init__(self, file):
        self.file = file
        self.file.filename = self.file.filename.lower()

    def __str__(self):
        return self.file.filename


# Models
class FileSubmission(BaseModel):
    file: UploadFile
    email: EmailStr
    storeName: str
    date_time: datetime | None
