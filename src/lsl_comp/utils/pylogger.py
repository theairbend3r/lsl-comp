import logging


def logger_creator(verbose: bool) -> logging.Logger:
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG
        )  # , format="%(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=logging.CRITICAL + 1)

    return logging.getLogger(__name__)
