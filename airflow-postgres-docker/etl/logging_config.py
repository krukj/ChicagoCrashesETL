import logging
from rich.logging import RichHandler


def setup_logger(name: str = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.hasHandlers():
        handler = RichHandler(
            show_time=True, show_level=True, show_path=True, rich_tracebacks=True
        )
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="[%H:%M:%S]"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
