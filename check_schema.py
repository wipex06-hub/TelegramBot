import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

print("Fetching schema...")
schema = con.execute("DESCRIBE SELECT * FROM read_parquet('hf://datasets/WipeX00/scrappeddata/idx_phone.*.parquet') LIMIT 1").fetchall()

for col in schema:
    print(col[0])
