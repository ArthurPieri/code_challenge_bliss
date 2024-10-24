from abc import ABC, abstractmethod

from .my_log import LoggingEtl


class PipelineInterface(ABC, LoggingEtl):
    """ """

    def __init__(self, **kwargs) -> None:
        """
        Initialize your class

        **Remember to always assert your kwargs, if you need any
        """
        self.log = LoggingEtl().start_logging()

    @abstractmethod
    def run(self, **kwargs):
        """
        Run your pipeline
        """

    @abstractmethod
    def extract(self, **kwargs):
        """
        Extract data from the source

        **Remember to always assert your kwargs, if you need any
        """

    @abstractmethod
    def transform(
        self, columns_to_drop: list = [], columns_to_rename: dict = {}, **kwargs
    ):
        """
        Transform your data

        Args:
        - columns_to_drop - a list of column names that will be dropped
        - columns_to_rename - a dict where the KEY is the column_name and the value is the renamed_column_name
            - example: { "UUUD": "user_id" }
                - UUID is the original name of the column
                - user_id is the new name for that column

        **Remember to always assert your kwargs
        """

    @abstractmethod
    def load(self, **kwargs):
        """
        Load data to destination

        Args:
        - merge_ids - a list of IDs that could be use when merge new_data with the existing data in a table

        **Remember to always assert your kwargs
        """
