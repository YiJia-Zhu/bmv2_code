import os
import sys
import traceback
from typing import Optional
import logging


logger = None


def setup_logging(log_dir: Optional[str] = "./logs/",
                  quiet: bool = False,
                  level=logging.INFO):
    # Create a logger
    global logger
    pid = os.getpid()
    logger = logging.getLogger(str(pid))
    logger.setLevel(level)

    # Check if the logger already has handlers to avoid duplicate logging
    if not logger.handlers:
        logger_format = '[%(asctime)s][%(name)s][%(levelname)s]: %(message)s'
        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)

            file_handler = logging.FileHandler(f"{log_dir}/{pid}.err")
            formatter = logging.Formatter(logger_format)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        if not quiet:
            stream_handler = logging.StreamHandler(sys.stderr)
            stderr_formatter = logging.Formatter(logger_format)
            stream_handler.setFormatter(stderr_formatter)
            logger.addHandler(stream_handler)

        # Prevent the logger from propagating messages to the root logger
        logger.propagate = False


def getLogger():
    spid = str(os.getpid())
    return logging.getLogger(spid)


def format_exception(e):
    estr = "".join(traceback.format_exception(e))
    return f"\n```\n{estr}```\n\n"
