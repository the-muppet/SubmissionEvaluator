import base64
import chardet
from pathlib import Path
import pandas as pd
from io import StringIO
from models import Submission
from utils.logger import setup_logger
from utils.config_manager import ConfigManager
from utils.normalizer import normalize_headers, handle_missing_data


logger = setup_logger()
column_types = {0: "str", 4: "str", 5: "str", 15: "str"}
config_manager = ConfigManager()


class DataLoader:
    def __init__(self, config_manager):
        self.config = config_manager
        self.sources = self.load_sources_config()

    def load_sources_config(self):
        """
        Loads data sources configuration from settings.ini.
        """
        sources = {
            "pullsheet": self.config.get(
                "DataLoader", "PULLSHEET", fallback="pullsheet.csv"
            ),
            "pullorder": self.config.get(
                "DataLoader", "PULLORDER", fallback="pullorder.csv"
            ),
            "catalog": self.config.get("DataLoader", "CATALOG", fallback="catalog.csv"),
        }
        return sources

    def read_csv_file(self, file_path, chunk_size=10000, dtype=None):
        """
        Reads a CSV file with dynamic encoding detection and optional chunking.

        Args:
            file_path (str): Path to the CSV file.
            chunk_size (int, optional): Number of rows per chunk. Use None to load the entire file at once.

        Returns:
            pd.DataFrame or Iterator[pd.DataFrame]: DataFrame if chunk_size is None, otherwise an iterator over DataFrames.
        """
        # Detect encoding
        raw_data = open(file_path, "rb").read()
        result = chardet.detect(raw_data)
        encoding = result["encoding"]

        # Ensure the file path is valid
        if not Path(file_path).is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            if chunk_size:
                return pd.read_csv(
                    file_path,
                    encoding=encoding,
                    chunksize=chunk_size,
                    dtype=column_types,
                )
            else:
                return pd.read_csv(file_path, encoding=encoding, dtype=column_types)
        except Exception as e:
            logger.error(f"Failed to read {file_path} due to: {e}")
            raise

    def load_data(self):
        """
        Loads all data sources specified in the configuration into DataFrames.
        """
        data_frames = {}
        for name, path in self.sources.items():
            try:

                data_frames[name] = self.read_csv_file(
                    path, chunk_size=None, dtype=column_types
                )
            except Exception as e:
                logger.error(f"Error loading data from {path}: {e}")
                continue
        return data_frames

    @staticmethod
    def from_csv(file_content, encoding_list=["utf-8", "ISO-8859-1", "utf-16"]):
        """
        Reads a CSV file content using different encodings until successful.

        Args:
            file_content (str): Content of the CSV file.
            encoding_list (list): List of encodings to try.

        Returns:
            pd.DataFrame: Loaded data as a DataFrame.
        """
        for encoding in encoding_list:
            try:
                column_types = {0: str, 4: str, 5: str, 15: str}
                df = pd.read_csv(
                    StringIO(file_content), encoding=encoding, dtype=column_types
                )
                # Apply normalization and handling missing data
                df = normalize_headers(df)
                df = handle_missing_data(df)
                return df
            except UnicodeDecodeError as e:
                logger.warning(f"Error with encoding {encoding}: {e}")
                continue
            except Exception as e:
                logger.error(f"Failed to load with encoding {encoding}: {e}")
                raise
        raise ValueError("Could not load the file with any of the tried encodings.")

    @classmethod
    def from_json(cls, json_data: dict, submission_class) -> Submission:
        try:
            csv_content = base64.b64decode(json_data["file_content"]).decode("utf-8")
            logger.info(f"Processing file upload from {json_data['store_name']}")
            df = pd.read_csv(StringIO(csv_content))
            # Assume normalize_headers and handle_missing_data are predefined functions
            df = normalize_headers(df)
            df = handle_missing_data(df)
            return submission_class(
                submission_df=df,
                store_name=json_data["store_name"],
                seller_email=json_data["email"],
            )
        except Exception as e:
            error_message = f"Failed to process the uploaded file: {e}"
            logger.error(error_message)
            # Depending on your error handling strategy, you might want to:
            # - Rethrow a custom exception
            # - Return an error object or None
            # - Handle the exception in another way
            raise ValueError(error_message) from e

    @staticmethod
    async def from_csv_stream(file_stream, chunk_size=1024 * 1024):
        """
        Asynchronously processes CSV data in chunks from a file stream, handling the header in the first chunk.

        Args:
            file_stream: An asynchronous file stream.
            chunk_size: Size of each chunk to read from the stream.

        Yields:
            DataFrames constructed from chunks of the CSV file, using the header from the first chunk.
        """
        buffer = ""
        header = None
        async for chunk in file_stream:
            text = chunk.decode("utf-8")
            buffer += text

            # Process the buffer while complete lines (ending in '\n') exist
            while "\n" in buffer:
                position = buffer.find("\n") + 1
                line, buffer = buffer[:position], buffer[position:]
                if header is None:
                    # The first line is assumed to be the header
                    header = line.strip().split(",")
                else:
                    # Process subsequent lines with the header
                    df = pd.read_csv(StringIO(line), header=None, names=header)
                    yield df

        # Handle any remaining data in the buffer
        if buffer and header is not None:
            df = pd.read_csv(StringIO(buffer), header=None, names=header)
            yield df
