
import duckdb

# Create an in-memory database
con = duckdb.connect()

# Create 2 million fake sales records
con.execute("""
    CREATE TABLE sales AS
    SELECT
        i AS id,
        ['US', 'UK', 'IN', 'DE'][1 + (i % 4)] AS country,
        (random() * 100)::DECIMAL(10, 2) AS revenue
    FROM range(2_000_000) t(i)
""")

print("Sales table created!")

# OLAP-shaped: aggregate revenue across all rows
result = con.execute("""
    SELECT country, SUM(revenue)
    FROM sales
    GROUP BY country
""").fetchall()

print("\nRevenue by country:")
print(result)

# OLTP-shaped: retrieve one specific order
result = con.execute("""
    SELECT *
    FROM sales
    WHERE id = 12345
""").fetchall()

print("\nOne specific order:")
print(result)

# Create a report containing US sales
con.execute("""
    CREATE TABLE report AS
    SELECT *
    FROM sales
    WHERE country = 'US'
""")

before = con.execute(
    "SELECT COUNT(*) FROM report"
).fetchone()[0]

# Deliberately append the same data a second time
con.execute("""
    INSERT INTO report
    SELECT *
    FROM sales
    WHERE country = 'US'
""")

after = con.execute(
    "SELECT COUNT(*) FROM report"
).fetchone()[0]

print("\nNon-idempotent example:")
print("Before:", before)
print("After duplicate append:", after)



# 2. Fix the problem
def refresh_us_report():
    con.execute("""
        DELETE FROM report
        WHERE country = 'US'
    """)

    con.execute("""
        INSERT INTO report
        SELECT *
        FROM sales
        WHERE country = 'US'
    """)

# 3. Verify the fix
refresh_us_report()
count_once = con.execute(
    "SELECT COUNT(*) FROM report"
).fetchone()[0]

refresh_us_report()
count_twice = con.execute(
    "SELECT COUNT(*) FROM report"
).fetchone()[0]

assert count_once == 500_000
assert count_twice == count_once

print("Idempotent example:")
print("After first refresh:", count_once)
print("After second refresh:", count_twice)
print("SUCCESS!")
