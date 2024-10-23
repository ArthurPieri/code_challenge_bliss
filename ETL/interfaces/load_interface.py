from abc import ABC, abstractmethod


from .my_log import LoggingEtl


class LoadInterface(ABC, LoggingEtl):
    """
    This class is used to load data to lake.
    It recieves a dataframe, a schema name, and a table name.
    Makes all the treatment necessary to load the data.
    """

    # Class is organized in this order:
    # 1. Abstract methods that should be implemented or overridable methods
    # 2. Non-abstract methods
    def __init__(self, **kwargs):
        """
        In the child class you should define what parameters to be used to get connection
        """
        LoggingEtl().__init__()

        self._get_connection(**kwargs)

    @abstractmethod
    def load(
        self,
        data: list[dict],
        merge_ids: list,
        **kwargs,
    ) -> None:
        """
        Load data to destination
        """

    def get_last_load_date(self, delta_date_columns: list = [], **kwargs):  # pylint: disable=dangerous-default-value
        """
        Get the last date from lake table.
        delta_date_columns: list of columns that will be used to filter last loaddate
        """
        self.log.info("Getting last date from table %s", kwargs["table"])

        assert delta_date_columns

        last_date = self._get_max_dates_from_table(delta_date_columns, **kwargs)

        if last_date:
            self.log.info("Last date from table %s is %s", kwargs["table"], last_date)

        return last_date

    @abstractmethod
    def _add_columns_to_table(self, columns_types: dict, **kwargs):
        """
        Add columns to table
        """

    @abstractmethod
    def _create_empty_table(self, columns_types: dict, **kwargs):
        """
        Create empty table on lake
        """

    @abstractmethod
    def _get_connection(self, **kwargs):
        """
        Parameters:
        - **Kwargs parameters are used to get connection
        """

    @abstractmethod
    def _get_max_dates_from_table(self, delta_date_columns, **kwargs):
        """This method should return the max date from lake table"""

    @abstractmethod
    def _load_data(
        self, columns_and_types: dict, data: list[dict], merge_ids: list, **kwargs
    ):
        """Load data to target"""

    @abstractmethod
    def _add_loaddate(self, **kwargs):
        """
        Add a loaddate column to the data.
        """

    def __del__(self):
        self.conn.close()  # pylint: disable=no-member # type: ignore
        self.log.info("Connection %s closed", self.__class__.__name__)
