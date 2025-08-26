from loguru import logger
logger.add("logs.log", rotation="10 MB", retention="10 days")
__all__ = ["logger"]
