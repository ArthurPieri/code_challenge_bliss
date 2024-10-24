"""
Read a json file and save it as parquet

## Future improvements
- TODO [ ] Set memory limit on duckdb configs
- TODO [ ] Break the file down into chunks
- TODO [ ] Check if file exists before trying to read it
"""

import duckdb
from utils.interfaces.pipeline import PipelineInterface


class JsonToDuck(PipelineInterface):
    def __init__(self, **kwargs):
        """
        Init pipeline
        """
        super().__init__()

        # Creating a few properties to check later
        self.len_extracted = {}
        self.len_loaded = {}
        self.len_transformed = {}
        # will be used to remove every temp table when pipeline is finished
        self.temp_tables = []

    def run(self, **kwargs): ...

    def extract(self, **kwargs):
        """
        Extract and make small transformations to data from json and save it to a Duckdb table

        ## Kwargs:
        - table_name:str =  is the name of the table to be created
        - json_file:str =  is the source file
        - select_query:str =  is part of the query used on the create table statement, it should not contain the FROM
            - Example: 'SELECT *'

        ## Exceptions
        - AssertionError - if one of the kwargs were invalid
        """
        assert "table_name" in kwargs
        assert type(kwargs["table_name"]) is str

        assert "json_file" in kwargs
        assert type(kwargs["json_file"] is str)

        assert "select_query" in kwargs
        assert type(kwargs["select_query"] is str)

        # Read from json and create temporary table
        self._create_table(
            from_clause=kwargs["json_file"],
            table_name=kwargs["table_name"],
            select_query=kwargs["select_query"],
            is_temp=True,
            operation="extract",
        )

    def transform(
        self, columns_to_drop: list = ..., columns_to_rename: dict = ..., **kwargs
    ):
        """ """

    def load(self, **kwargs):
        """
        Save your duckdb table to a parquet file

        ## Kwargs:
        - append: bool = if True then the data will be appended to file
            - if False, will eliminate duplicates
        - table_name: str =  name of the table to be created
        - filemame: str =  is the name of the parquet file
            - it can be the same as the table_name or can be a partition

        ## Exceptions
        - AssertionError - if one of the kwargs are invalid
        """
        assert "table_name" in kwargs
        assert "filename" in kwargs
        assert "append" in kwargs

        # Add loaddate to table
        self._add_loaddate(table_name=kwargs["table_name"])

        try:
            duckdb.sql(f"SELECT * FROM {kwargs["filename"]} limit 1").fetchone()
            self.log.info("File: %s foumd, merging data into it", kwargs["filename"])
            self._merge_tables(
                append=kwargs["append"],
                filename=kwargs["filename"],
                table_name=kwargs["table_name"],
            )
        except Exception as exc:
            self.log.debug(exc)
            self.log.info("File: %s not found, creating it", kwargs["filename"])
            self._create_parquet_file(
                table_name=kwargs["table_name"], filename=kwargs["filename"]
            )

    def _add_loaddate(self, table_name: str):
        """
        Add column Loaddate to table

        Args:
        - table_name: the name of the table data will be loadded to
        """
        query = f"""
        ALTER TABLE {table_name}
        ADD COLUMN loaddate TIMESTAMP;
        """
        duckdb.sql(query)

        update = f"""
        UPDATE {table_name}
        SET loaddate = CURRENT_TIMESTAMP;
        """
        duckdb.sql(update)

    def _create_table(
        self,
        from_clause: str,
        table_name: str,
        select_query: str,
        operation: str,
        is_temp: bool = True,
    ):
        """
        Create table

        ## Args:
        - operation:
        - table_name:
        - select_query:
        - is_temp:
        """
        if is_temp:
            table_name += "_temp"
            self.temp_tables.append(table_name)

        query = f"CREATE TABLE {table_name} AS "
        query += select_query
        query += f" FROM {from_clause}"
        duckdb.sql(query)

        size = self._get_len(table_name=table_name, operation=operation)

        self.log.info(
            "Table: %s created with %s line(s)",
            "table_name",
            str(size),
        )

    def _create_parquet_file(self, table_name: str, filename: str):
        """
        Create parquet file

        ## Args
        - table_name: name of the table to be exported
        - filename: filename where to save
        """
        if ".parquet" not in filename:
            raise SyntaxError("filename must be a .parquet file")

        duckdb.table(table_name).to_parquet(filename)
        self.log.info("Table: %s saved on file: %s", table_name, filename)

    def _get_len(self, table_name: str, operation: str):
        """
        Get number of rows in a table
        """
        size = duckdb.sql(f"SELECT count(*) FROM {table_name}").fetchone()

        if operation == "extract":
            self.len_extracted[table_name] = size[0]  # type: ignore
        elif operation == "load":
            self.len_loaded[table_name] = size[0]  # type: ignore
        elif operation == "transform":
            self.len_transformed[table_name] = size[0]  # type: ignore
        else:
            raise AttributeError("Operation not recognized")

        return size

    def _merge_tables(self, filename: str, table_name: str, append: bool):
        """
        Read the parquet file, and merge the temp table to import

        ## Args:
        - filename
        - table_name
        """
        # Read parquet file and save to table_old
        old_table = f"{table_name}_old"
        self._create_table(
            from_clause=filename,
            table_name=old_table,
            select_query="SELECT *",
            is_temp=False,
            operation="extract",
        )
        self.temp_tables.append(old_table)

        # Union
        union_clause = "UNION BY NAME"
        if append:
            union_clause = "UNION ALL BY NAME"

        merged_table = f"{table_name}_merged"
        # Merge tables
        duckdb.sql(f"""
        CREATE TABLE {merged_table} AS
        SELECT * FROM {old_table}
        {union_clause}
        SELECT * FROM {table_name}
        """)

        self._get_len(table_name=merged_table, operation="load")
        self.log.info(
            "Tables: %s e %s merged successfully. Total rows: %s",
            old_table,
            table_name,
            self.len_loaded[merged_table],
        )

        self._create_parquet_file(table_name=merged_table, filename=filename)

        # Drop tables
        for table in [old_table, table_name, merged_table]:
            duckdb.sql(f"DROP TABLE {table}")

    def __del__(self):
        for table in self.temp_tables:
            duckdb.sql(f"DROP TABLE {table}")
