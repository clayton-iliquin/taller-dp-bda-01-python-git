

# modules.utils.logger.py
# ==================================================
# common
import sys
import logging
import inspect
# --------------------------------------------------

def get_console_handler() -> 'logging.StreamHandler':
    console_format = logging.Formatter("%(filename)s:%(lineno)d - %(levelname)s — %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_format)
    return console_handler

def get_inspect() -> str:
    return inspect.stack()[1][3]

def get_logger() -> 'logging.Logger':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.propagate = False
    return logger
