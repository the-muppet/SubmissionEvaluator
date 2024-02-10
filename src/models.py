import uuid
import pandas as pd
from datetime import datetime
from dataclasses import dataclass

# Submission class
@dataclass(frozen=True)
class Submission:
    dataframe: pd.DataFrame
    uuid: str
    store_name: str
    seller_email: str
    creation_time: str

    @classmethod
    def new_submission(cls, dataframe, store_name, seller_email):
        return cls(
            dataframe=dataframe,
            uuid=uuid.uuid4().__str__(),
            store_name=store_name,
            seller_email=seller_email,
            creation_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )