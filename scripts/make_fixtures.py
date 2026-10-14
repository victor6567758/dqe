"""Regenerates the test fixtures in src/test/resources.

sales.csv and sales.parquet: the 8-row SALES table.
customers.csv, orders.csv, items.parquet: the three-table join dataset.
  Orders with id divisible by 17 have no customer (empty customer_id, which loads as NULL).
  Customer ids 41 to 45 do not exist, so their orders drop out of inner joins.

Requires: pip install pyarrow
Run from the project root: python3 scripts/make_fixtures.py
"""
import csv
import pathlib
import random

import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(__file__).resolve().parent.parent / "src" / "test" / "resources"
out.mkdir(parents=True, exist_ok=True)

# SALES: the 8-row sample
SALES = [("east", 10), ("west", 20), ("east", 5), ("west", 7),
         ("north", 3), ("east", 4), ("north", 8), ("west", 1)]
with open(out / "sales.csv", "w", newline="") as f:
    f.write("region,sales\n")
    f.writelines(f"{region},{sales}\n" for region, sales in SALES)
table = pa.table({
    "region": [region for region, _ in SALES],
    "sales": pa.array([sales for _, sales in SALES], type=pa.int64()),
})
pq.write_table(table, out / "sales.parquet", compression="none")

# Join dataset
COUNTRIES = ["DE", "FR", "US", "CA", "JP"]
PRODUCTS = ["bolt", "gear", "spring", "axle", "shaft", "bearing"]
STATUSES = ["new", "shipped", "cancelled"]
rnd = random.Random(11)

customers = [(i, f"customer-{i}", rnd.choice(COUNTRIES)) for i in range(1, 41)]

orders = []
for i in range(1, 201):
    customer = None if i % 17 == 0 else 1 + rnd.randrange(45)
    orders.append((i, customer, 10 + rnd.randrange(200), rnd.choice(STATUSES)))

items = []
for _ in range(600):
    items.append((1 + rnd.randrange(200), rnd.choice(PRODUCTS),
                  1 + rnd.randrange(5), 1 + rnd.randrange(50)))

with open(out / "customers.csv", "w", newline="") as f:
    writer = csv.writer(f, lineterminator="\n")
    writer.writerow(["id", "name", "country"])
    writer.writerows(customers)

with open(out / "orders.csv", "w", newline="") as f:
    writer = csv.writer(f, lineterminator="\n")
    writer.writerow(["id", "customer_id", "amount", "status"])
    for order_id, customer, amount, status in orders:
        writer.writerow([order_id, "" if customer is None else customer, amount, status])

pq.write_table(pa.table({
    "order_id": pa.array([row[0] for row in items], type=pa.int64()),
    "product": [row[1] for row in items],
    "quantity": pa.array([row[2] for row in items], type=pa.int64()),
    "unit_price": pa.array([row[3] for row in items], type=pa.int64()),
}), out / "items.parquet", compression="none")

print("wrote fixtures to", out)
