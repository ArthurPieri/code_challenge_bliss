"""
Read a json file and save it as parquet

## Future improvements
- TODO [ ] Set memory limit on duckdb configs
- TODO [ ] Break the file down into chunks
- TODO [ ] Check if file exists before trying to read it
- TODO [ ] Make run() accept a list of dicts to run multiple files at once
- TODO [ ] Add validation methods to the data being extracted (e.g GreatExpectations)
"""

# %%
import base64
import os
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from utils.interfaces.pipeline import PipelineInterface


# %%
class JsonToDuck(PipelineInterface):
    def __init__(self, **kwargs):
        """
        Init pipeline
        """
        super().__init__()
        self.start_time = time.time()

        # Creating a few properties to check later
        self.extracted = {}
        self.loaded = {}
        self.transformed = {}
        self.pipeline_run_time = 0.0
        # will be used to remove every temp table when pipeline is finished
        self.temp_tables = []

    def extract(self, **kwargs) -> str:
        """
        Extract and make small transformations to data from json and save it to a Duckdb table

        ## Kwargs:
        - is_encrypted: bool = if the source file is encrypted
        - source_file:str =  is the source file
        - select_query:str =  is part of the query used on the create table statement, it should not contain the FROM
            - Example: 'SELECT *'
        - table_name:str =  is the name of the table to be created
        - is_encrypted: bool = Is the source file encrypted?
        - password: str = Password to access the file

        ## Exceptions
        - AssertionError - if one of the kwargs were invalid
        """
        assert "source_file" in kwargs
        assert type(kwargs["source_file"] is str)
        assert "select_query" in kwargs
        assert type(kwargs["select_query"] is str)
        assert "table_name" in kwargs
        assert type(kwargs["table_name"]) is str
        assert "is_encrypted" in kwargs

        start_time = time.time()

        if kwargs["is_encrypted"]:
            assert kwargs["password"]
            temp_table_name = self._decrypt_file(
                filename=kwargs["source_file"],
                is_temp=True,
                password=kwargs["password"],
                select_query=kwargs["select_query"],
                table_name=kwargs["table_name"],
            )
        else:
            temp_table_name = self._create_table(
                from_clause=kwargs["source_file"],
                table_name=kwargs["table_name"],
                select_query=kwargs["select_query"],
                is_temp=True,
                operation="extract",
            )

        end_time = time.time()
        self.extracted[temp_table_name]["elapsed_time"] = format(
            end_time - start_time, ".3f"
        )

        return temp_table_name

    def load(self, **kwargs):
        """
        Save your duckdb table to a parquet file

        ## Kwargs:
        - append: bool = if True then the data will be appended to file
            - if False, will eliminate duplicates
        - filemame: str =  is the name of the parquet file
            - it can be the same as the table_name or can be a partition
        - is_encrypted: bool = if the destination file should be encrypted
        - table_name: str =  name of the table to be created
        - temp_table_name: str = name of the temporary table created on self.extract()
        - is_encrypted: bool = Is the source file encrypted?
            - password: str = Password to access the file

        ## Exceptions
        - AssertionError - if one of the kwargs are invalid
        """
        assert "append" in kwargs
        assert "filename" in kwargs
        assert "table_name" in kwargs
        assert "temp_table_name" in kwargs

        if kwargs["is_encrypted"]:
            assert kwargs["password"]

        start = time.time()
        # Add loaddate to table
        self._add_loaddate(table_name=kwargs["temp_table_name"])

        try:
            duckdb.sql(f"SELECT * FROM {kwargs["filename"]} limit 1").fetchone()
            self.log.info("File: %s found, merging data into it", kwargs["filename"])
            self._merge_tables(
                append=kwargs["append"],
                filename=kwargs["filename"],
                table_name=kwargs["table_name"],
                temp_table_name=kwargs["temp_table_name"],
                is_encrypted=kwargs["is_encrypted"],
            )
        except Exception as exc:
            self.log.debug(exc)
            self.log.info("File: %s not found, creating it", kwargs["filename"])
            self._create_parquet_file(
                table_name=kwargs["table_name"],
                filename=kwargs["filename"],
                is_encrypted=kwargs["is_encrypted"],
                password=kwargs["password"],
            )
        finally:
            end = time.time()
            table = kwargs["table_name"]
            if not self.loaded.get(table):
                self.loaded[table] = {"elapsed_time": 0.0}

            self.loaded[table]["elapsed_time"] = format(end - start, ".3f")

    def run(self, execution_list: list[dict]) -> dict:
        """
        Run the pipeline for multiple files

        ## Kwargs:
        - execution_list is a dict that contains the following args:
            - append: bool = if True then the data will be appended to file
            - filemame: str =  is the name of the parquet file
            - source_file: str =  is the source file
            - select_query: str =  is part of the query used on the create table statement, it should not contain the FROM
                - Example: 'SELECT *'
            - table_name: str =  name of the table to be created
            - is_encrypted: bool = Is the source file encrypted?
                - password: str = Password to access the file

        ## Exceptions
        - AssertionError

        """
        assert type(execution_list) is list

        start_time = time.time()

        for d in execution_list:
            assert type(d) is dict
            assert "append" in d
            assert "is_encrypted" in d
            assert "filename" in d
            assert "source_file" in d
            assert "select_query" in d
            assert "table_name" in d

            if d["is_encrypted"]:
                assert "password" in d

            self.log.info("===== STARTING RUN FOR %s =====", d["table_name"].upper())
            # Extract the data from file
            temp_table_name = self.extract(
                table_name=d["table_name"],
                source_file=d["source_file"],
                select_query=d["select_query"],
                is_encrypted=d["is_encrypted"],
                password=d["password"]
            )
            # Apply transformations to data

            # Load data to File
            self.load(
                table_name=d["table_name"],
                temp_table_name=temp_table_name,
                append=d["append"],
                filename=d["filename"],
                is_encrypted=d["is_encrypted"],
                password=d["password"]
            )

            self.log.info(
                "Extracted: %s, Transformed: %s, Total_Loaded: %s",
                self.extracted.get(temp_table_name, {}).get("rows"),
                self.transformed.get(temp_table_name, {}).get("rows"),
                self.loaded.get(d["table_name"], {}).get("rows"),
            )
            self.log.info("===== RUN ENDED FOR %s =====", d["table_name"].upper())

        end_time = time.time()
        self.pipeline_run_time = format(end_time - start_time, ".3f")

        return self._get_statistics()

    def transform(
        self, columns_to_drop: list = ..., columns_to_rename: dict = ..., **kwargs
    ):
        """ """

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
        table_name: str,
        select_query: str,
        operation: str,
        is_temp: bool,
        data=None,
        from_clause: str | None = None,
    ):
        """
        Create table

        ## Args:
        - is_temp: is a temporary table?
        - operation: ['extract', 'load', 'transform']
        - select_query: The select part of the query to create the table, do not include FROM
        - table_name: name of the table to be created
        """
        if not data:
            assert from_clause

        if is_temp:
            table_name += "_temp"
            self.temp_tables.append(table_name)

        query = f"CREATE TABLE {table_name} AS "
        query += select_query
        if data:
            query += f" FROM {data}"
        else:
            query += f" FROM '{from_clause}'"
        duckdb.sql(query)

        size = self._get_len(table_name=table_name, operation=operation)

        self.log.info(
            "Table: %s created with %s line(s)",
            table_name,
            str(size[0]),  # type: ignore
        )
        return table_name

    def _create_parquet_file(
        self,
        table_name: str,
        filename: str,
        is_encrypted: bool = False,
        password: str = "",
    ):
        """
        Create parquet file

        ## Args
        - filename: filename where to save
        - table_name: name of the table to be exported
        """
        if ".parquet" not in filename:
            raise SyntaxError("filename must be a .parquet file")

        if is_encrypted:
            self._encrypt_file(table_name=table_name, password=password)
        else:
            duckdb.table(table_name).to_parquet(filename)

        self.log.info("Table: %s saved on file: %s", table_name, filename)

    def _decrypt_file(
        self,
        filename: str,
        is_temp: bool,
        password: str,
        select_query: str,
        table_name: str,
    ) -> str:
        """ """
        with open(filename, "rb") as f:
            salt = f.read(16)
            encrypted_data = f.read()

        key, _ = self.__generate_key_from_password(password, salt)
        fernet = Fernet(key)

        decrypted_data = fernet.decrypt(encrypted_data)
        buffer = pa.BufferReader(decrypted_data)
        data = pq.read_table(buffer)

        return self._create_table(
            data=data,
            is_temp=is_temp,
            operation="extract",
            select_query=select_query,
            table_name=table_name,
        )

    def _encrypt_file(self, password: str, table_name: str) -> None:
        """
        Encrypt the parquet file

        ## Args:
        - filename: is the file to be writen
        """
        table = pq.read_table(table_name)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        parquet_bytes = buffer.getvalue().to_pybytes()
        key, salt = self.__generate_key_from_password(password)
        fernet = Fernet(key)
        encrypted_data = fernet.encrypt(parquet_bytes)
        output_file = f"{table_name}.crypt.parquet"
        with open(output_file, "wb") as f:
            f.write(salt)
            f.write(encrypted_data)

    def _get_len(self, table_name: str, operation: str):
        """
        Get number of rows in a table
        """
        size = duckdb.sql(f"SELECT count(*) FROM {table_name}").fetchone()

        if operation == "extract":
            self.extracted[table_name] = {"rows": size[0]}  # type: ignore
        elif operation == "load":
            self.loaded[table_name] = {"rows": size[0]}  # type: ignore
        elif operation == "transform":
            self.transformed[table_name] = {"rows": size[0]}  # type: ignore
        else:
            raise AttributeError("Operation not recognized")

        return size

    def _get_statistics(self) -> dict:
        """
        Get statistics from class
        """
        return {
            "extracted": self.extracted,
            "loaded": self.loaded,
            "transformed": self.transformed,
            "pipeline_time": self.pipeline_run_time,
        }

    def _merge_tables(
        self,
        filename: str,
        table_name: str,
        temp_table_name: str,
        append: bool,
        is_encrypted: bool,
    ):
        """
        Read the parquet file, and merge the temp table to import

        ## Args:
        - filename
        - table_name
        - temp_table_name
        """
        # Read parquet file and save to table_old
        old_table = f"{table_name}_old"
        start = time.time()
        self._create_table(
            from_clause=filename,
            table_name=old_table,
            select_query="SELECT *",
            is_temp=False,
            operation="extract",
        )
        self.temp_tables.append(old_table)
        end = time.time()

        if not self.extracted.get(old_table):
            self.extracted[old_table] = {"elapsed_time": 0.0}
        self.extracted[old_table]["elapsed_time"] = format((end - start), ".3f")

        if append:
            union_clause = "UNION ALL BY NAME"
        else:
            union_clause = "UNION BY NAME"

        # Merge tables
        duckdb.sql(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM {old_table}
        {union_clause}
        SELECT * FROM {temp_table_name}
        """)

        self._get_len(table_name=table_name, operation="load")
        self.log.info(
            "Tables: %s e %s merged into: %s successfully. Total rows: %s",
            old_table,
            temp_table_name,
            table_name,
            self.loaded[table_name]["rows"],
        )

        assert (
            self.extracted[temp_table_name]["rows"] + self.extracted[old_table]["rows"]
        ) == self.loaded[table_name]["rows"]

        self._create_parquet_file(
            table_name=table_name, filename=filename, is_encrypted=is_encrypted
        )

        # Drop tables
        for table in [old_table, temp_table_name, table_name]:
            duckdb.sql(f"DROP TABLE {table}")
            if table in self.temp_tables:
                self.temp_tables.remove(table)

    def __generate_key_from_password(
        self, password: str, salt: bytes | None = None
    ) -> tuple[bytes, bytes]:
        """ """
        assert password

        if not salt:
            salt = os.urandom(16)

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100
        )

        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))

        return key, salt

    def __del__(self):
        for table in self.temp_tables:
            try:
                duckdb.sql(f"DROP TABLE {table}")
            except Exception as exc:
                pass
        end_time = time.time()
        self.log.info(
            "Class ran for: %s second(s)",
            str(format(end_time - self.start_time, ".3f")),
        )
        self.log.info("===== PIPELINE COMPLETED =====\n")


# %%
if __name__ == "__main__":
    jd = JsonToDuck()
    jd.run(
        [
            {
                "table_name": "payments",
                "source_file": "../payments.json",
                "select_query": "SELECT cast(payment_id as INTEGER) as payment_id, CAST(transaction_id as INTEGER) as transaction_id, amount_paid, date_paid",
                "append": False,
                "filename": "payments.parquet",
                "is_encrypted": False,
                "password": "123456",
            },
            {
                "table_name": "transactions",
                "source_file": "transactions.parquet",
                "select_query": "SELECT cast(customer_id as VARCHAR) as customer_id, CAST(transaction_id as INTEGER) as transaction_id, amount, date",
                "append": False,
                "filename": "transactions.parquet",
                "is_encrypted": False,
                "password": "123456",
            },
        ]
    )
