# %%
import duckdb 

# %%
duckdb.sql("select * from 'payments.parquet' order by payment_id, loaddate")
