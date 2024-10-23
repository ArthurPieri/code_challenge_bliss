# %%
import json
from pprint import pformat
import sqlite3
import os

# %%
with open('payments.json', 'r') as file:
    data = json.load(file)
print(pformat(data[:10]))
print(len(data))

# %%
with open('transactions.json', 'r') as file:
    data = json.load(file)
print(pformat(data[:10]))
print(len(data))

# %% Save to sqlite3
current_file_path = os.path.abspath(__file__)
project_root = os.path.dirname(current_file_path)
file_path = os.path.join

# def _get_connection(self, **kwargs) -> object:
#     if "db_name" not in kwargs:
#         raise KeyError("db_name not provided")
#     db_name = f"{kwargs["db_name"]}.db"
#     file_path = os.path.join(self.project_root, "db/")
#     if not os.path.exists(file_path):
#         os.makedirs(file_path)
#     file_path = os.path.join(file_path, db_name)
#     return sqlite3.connect(file_path)
#

