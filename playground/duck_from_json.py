# %%
import duckdb

# %%
duckdb.sql(""" 
CREATE OR REPLACE TABLE payments AS 
SELECT
    CAST(payment_id AS INTEGER) AS payment_id,
    CAST(transaction_id AS INTEGER) AS transaction_id,
    amount_paid,
    date_paid
    FROM payments.json
""")

# %%
size = duckdb.sql("select count(*) from payments").fetchone()
print(size)

# %%
print(type(size))

# %%
size = size[0]
print(size)

# %% 
table_name = "payments"

# %%
query = f"""
ALTER TABLE {table_name}
ADD COLUMN loaddate TIMESTAMP;
"""
duckdb.sql(query)

# %%
duckdb.table(table_name).show()

# %%
duckdb.sql("""
DELETE FROM payments where payment_id > 50
""")

# %%
update = f"""
UPDATE {table_name}
SET loaddate = CURRENT_TIMESTAMP;
"""
duckdb.sql(update)

# %%
duckdb.table(table_name).show()

# %%
duckdb.sql("""
DELETE FROM payments where payment_id > 50
""")

# %%
duckdb.table(table_name).show()

# %%
duckdb.sql("""
COPY payments TO 'payments.parquet' (FORMAT PARQUET);
""")

# %%
a = duckdb.sql("""
SELECT payment_id, loaddate FROM payments.parquet
""").fetchone()

# %%
duckdb.table(table_name).to_parquet("payments.parquet")

# %%
duckdb.sql("""
SELECT payment_id, loaddate FROM payment123s.parquet
""")


# %%
try:
    a = duckdb.sql("""
SELECT count(*) FROM payments.parquet
""")
    print(a)
except Exception as e:
    print(e)

# %%
filename = "payments.parquet"

if ".parquet" in filename:
    print("reconheceu")
