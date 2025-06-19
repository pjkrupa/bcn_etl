import logging

def get_logger(
        name='__name__', 
        path='etl.log', 
        level=logging.INFO,
        ):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    file_handler = logging.FileHandler(
        filename="etl.log",
        encoding="utf-8",
        mode="a",
    )
    console_handler = logging.StreamHandler()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    formatter = logging.Formatter( 
        "{asctime} - {levelname} - {message}", 
        style="{",
        datefmt="%H:%M:%S",
    )

    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    return logger