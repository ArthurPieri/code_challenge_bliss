import logging


class LoggingEtl:
    """
    Centralizing logging into a class.
    """

    def start_logging(self):
        """
        Start logging for class.
        """
        logging.basicConfig(
            filename=f"{self.__class__.__name__}.log",
            encoding="utf-8",
            level=logging.DEBUG,
            format="[%(levelname)s]%(filename)s:%(lineno)d %(asctime)s - %(message)s",
        )
        return logging.LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {"class": self.__class__.__name__},
        )


#     logging.shutdown()
