#!/usr/bin/env python3
"""Quick debug: break down Feb 2024 revenue by type."""
import csv, os

DIR = os.path.expanduser("~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU")

def money(v):
    return float((v or "0").replace("$", "").replace(",", ""))

# Load orders into dict
print("Loading orders...")
orders = {}
with open(f"{DIR}/orders.csv") as f:
    for r in csv.DictReader(f):
        orders[r["id"]] = r
print(f"  {len(orders)} orders loaded")

# Load passes into dict
print("Loading passes...")
passes = {}
with open(f"{DIR}/passes.csv") as f:
    for r in csv.DictReader(f):
        passes[r["id"]] = r
print(f"  {len(passes)} passes loaded")

# Step 2: Non-subscription passes
pass_order_ids = set()
pass_total = 0
pass_count = 0
for pid, p in passes.items():
    cat = (p.get("pass_category_name") or "").lower()
    total = money(p.get("total"))
    oid = p.get("order_id", "")
    if cat == "subscription" or total <= 0 or not oid:
        continue
    o = orders.get(oid)
    if not o:
        continue
    d = o.get("completed_at", "")
    if d.startswith("2024-02") and o.get("state", "") in ("completed", "refunded"):
        pass_order_ids.add(oid)
        pass_total += total
        pass_count += 1

# Step 3: Non-pass orders
sub_total = 0
sub_count = 0
other_total = 0
other_count = 0
for oid, o in orders.items():
    d = o.get("completed_at", "")
    total = money(o.get("total"))
    if not d.startswith("2024-02") or o.get("state", "") not in ("completed", "refunded") or total <= 0:
        continue
    if oid in pass_order_ids:
        continue
    if o.get("subscription_pass_id", ""):
        sub_total += total
        sub_count += 1
    else:
        other_total += total
        other_count += 1

print(f"\n=== Feb 2024 Revenue Breakdown ===")
print(f"Pass revenue (non-sub passes):  {pass_count:5d} items  ${pass_total:>12,.2f}")
print(f"Subscription renewal orders:    {sub_count:5d} items  ${sub_total:>12,.2f}")
print(f"Other non-pass orders:          {other_count:5d} items  ${other_total:>12,.2f}")
print(f"                                -----        ------------")
print(f"TOTAL:                          {pass_count + sub_count + other_count:5d} items  ${pass_total + sub_total + other_total:>12,.2f}")
print(f"\nAdmin report total:                              $   84,197.33")
print(f"Gap:                                             ${pass_total + sub_total + other_total - 84197.33:>12,.2f}")
