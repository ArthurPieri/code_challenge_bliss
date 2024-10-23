"""
Create the interface to DB_conn
"""

from abc import ABC, abstractmethod
from .my_log import LoggingEtl

class Interface_DB(ABC, LoggingEtl):
    def __init__(self) -> None:
        LoggingEtl().__init__()

    @abstractmethod
    def create(self):
        """
        Create db if not exists
        """

    @abstractmethod
    def execute(self, sql: str, **kwargs) -> None:
        """
        Run the sql query with no response
        """

    @abstractmethod
    def fetch_one(self, sql: str, **kwargs):
        """
        Run sql and get one result
        """

    @abstractmethod
    def fetch_all(self, sql: str, **kwargs):
        """
        Run sql and get all results
        """

    @abstractmethod
    def fetch_many(self, sql: str, limit: int, **kwargs):
        """
        Run sql and get one result
        """

    @abstractmethod
    def _get_connection(self, **kwargs) -> object:
        """
        Kwargs arguments shall be used for connecting to DB
        """
