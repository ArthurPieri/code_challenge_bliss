# pylint: disable=import-error, too-few-public-methods
from abc import ABC, abstractmethod

from .my_log import LoggingEtl


class TransformInterface(ABC, LoggingEtl):
    """
    This class is used to transform data to be sent.
    It recieves a list of dicts and makes all
    the treatments necessary to adapt the data to the destination
    It returns a list of dicts.
    """

    @abstractmethod
    def transform(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        columns_to_drop: list = [],
        columns_to_rename: dict = {},
        **kwargs,
    ) -> list[dict]:
        """
        Transform data from source and return a list of dicts
        """
