import uuid
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
from pydantic import BaseModel, EmailStr


# Pydantic Models
class SubmissionResponse(BaseModel):
    uuid: str
    store_name: str
    seller_email: EmailStr
    creation_time: str


class ClientIdResponse(BaseModel):
    client_id: str


# Submission class
@dataclass(frozen=True)
class Submission:
    dataframe: pd.DataFrame
    uuid: str
    store_name: str
    seller_email: str
    creation_time: str

    @classmethod
    def new_submission(
        cls, dataframe: pd.DataFrame, store_name: str, seller_email: EmailStr
    ):
        return cls(
            dataframe=dataframe,
            uuid=str(uuid.uuid4()),
            store_name=store_name,
            seller_email=seller_email,
            creation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
