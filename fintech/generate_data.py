import random, uuid, os
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

N_CUSTOMERS = 2000
N_ROWS = 100_000
START = datetime(2026, 10, 1)
DAYS = 22

# 1. Customers
customers = [
    {"customer_id": f"C{i:05d}", "customer_name": fake.name(),
     "customer_email": fake.email(), "iban": fake.iban()}
    for i in range(1, N_CUSTOMERS + 1)
]

inflow_desc = ["SALARY OCT 2026", "Salary payment", "SEPA transfer received", "Refund", "Cash deposit"]
outflow_desc = ["Grocery", "SEPA transfer sent", "Rent", "Online shopping",
                "Utility bill", "Restaurant", "Transaction reversal"]

# 2. Transactions
rows = []
for _ in range(N_ROWS):
    c = random.choice(customers)
    ts = START + timedelta(days=random.randint(0, DAYS - 1),
                           seconds=random.randint(0, 86399))
    if random.random() < 0.35:  # inflow
        desc = random.choice(inflow_desc)
        amount = round(random.uniform(2000, 5000), 2) if "alary" in desc.lower() \
                 else round(random.uniform(10, 1500), 2)
    else:                       # outflow
        desc = random.choice(outflow_desc)
        amount = -round(random.uniform(5, 800), 2)

    rows.append({
        "transaction_id": str(uuid.uuid4()),
        "source_account_iban": c["iban"],
        "target_account_iban": fake.iban(),
        "amount": amount,
        "currency": random.choice(["EUR", "USD", "QAR"]),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "description": desc,
        "customer_id": c["customer_id"],
        "customer_email": c["customer_email"],
        "customer_name": c["customer_name"],
        "status": random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"]),
    })

df = pd.DataFrame(rows)

# 3. Jaan-bujh ke gandha data
dupes = df.sample(frac=0.01, random_state=1)           # 1% duplicate rows
df = pd.concat([df, dupes], ignore_index=True)
df.loc[df.sample(frac=0.005, random_state=2).index, "description"] = None  # missing description

# 4. Din ke hisaab se alag folders mein save karo
df["_date"] = pd.to_datetime(df["timestamp"]).dt.date
for d, g in df.groupby("_date"):
    folder = f"out/transactions/year={d.year}/month={d.month:02d}/day={d.day:02d}"
    os.makedirs(folder, exist_ok=True)
    g = g.drop(columns="_date")
    g.to_csv(f"{folder}/transactions_{d:%Y%m%d}.csv", index=False)
    g.to_json(f"{folder}/transactions_{d:%Y%m%d}.json", orient="records", lines=True)

print("Done. Total rows:", len(df))