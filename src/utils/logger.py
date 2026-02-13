import logging
import os

LOG_BASE_DIR = "logs"

def setup_logger(module_name, folder, filename, level=logging.INFO):
    log_dir = os.path.join(LOG_BASE_DIR, folder)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(module_name)
    logger.setLevel(level)

    if not logger.handlers:
        file_handler = logging.FileHandler(
            os.path.join(log_dir, filename),
            encoding="utf-8"
        )

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
