import os
import argparse
from io import StringIO
from src.data_loader import DataLoader
from src.file_handler import FileHandler
from src.utils.logger import setup_logger
from src.utils.config_manager import ConfigManager
from src.evaluator import Evaluator, Submission, EvalFrame


logger = setup_logger()
config = ConfigManager()


def process_submission(file_path=None, file_content=None, store_name=None, email=None):
    try:
        if file_path:
            # Load from file system
            submission_df = DataLoader(config).read_csv_file(file_path)
        elif file_content:
            # Load from web upload
            submission_df = DataLoader(config).from_csv(StringIO(file_content))

        # Assuming store_name and email are available or not required for direct drops
        submission = Submission.new_submission(submission_df, store_name, email)
        eval_frame = EvalFrame(submission, DataLoader(config))
        evaluation_df = eval_frame.evaluation_df
        evaluator = Evaluator(evaluation_df)

        return evaluator.results_to_dict()
    except Exception as e:
        logger.error(f"Failed to process submission: {e}")
        return None


def process_file(file_path):
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return
    logger.info(f"Processing file: {file_path}")
    result = process_submission(file_path=file_path)
    if result:
        logger.info(f"File {file_path} processed successfully: {result}")
    else:
        logger.error(f"Failed to process the file {file_path}")


def monitor_directory():
    directory_path = config.get("FileHandler", "WATCHDIR")
    logger.info(f"Starting to monitor {directory_path} for new submissions...")
    handler = FileHandler(directory_path, process_submission)
    handler.start()


def main():
    parser = argparse.ArgumentParser(description="Submission File Processor")
    parser.add_argument("--file", help="Path to a specific submission file to process")
    parser.add_argument(
        "--monitor", help="Enable directory monitoring", action="store_true"
    )
    args = parser.parse_args()

    if args.monitor:
        monitor_directory()
    elif args.file:
        process_file(args.file)
    else:
        logger.error("No file or directory specified for processing.")
        parser.print_help()


if __name__ == "__main__":
    main()
