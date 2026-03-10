#!/usr/bin/env python3
"""Debug: check sub renewal payment methods and sales channels."""
import csv, os
from collections import Counter

DIR = os.path.expanduser("~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU")

def money(v):
    return float((v or "0").replace("$", "").replace(",", ""))

orders = {}
with open(f"{DIR}/orders.csv") as f:
    for r in csv.DictReader(f):
        orders[r["id"]] = r

passes = {}
with open(f"{DIR}/passes.csv") as f:
    for r in csv.DictReader(f):
        passes[r["id"]] = r

# Non-sub pass order IDs
pass_order_ids = set()
for pid, p in passes.items():
    cat = (p.get("pass_category_name") or "").lower()
    total = money(p.get("total"))
    oid = p.get("order_id", "")
    if cat == "subscription" or total <= 0 or not oid:
        continue
    o = orders.get(oid)
    if not o: continue
    d = o.get("completed_at", "")
    if d.startswith("2024-02") and o.get("state", "") in ("completed", "refunded"):
        pass_order_ids.add(oid)

# Analyze sub renewal orders
payment_methods = Counter()
sales_channels = Counter()
total_by_method = {}
for oid, o in orders.items():
    d = o.get("completed_at", "")
    total = money(o.get("total"))
    if not d.startswith("2024-02") or o.get("state", "") not in ("completed", "refunded") or total <= 0:
        continue
    if oid in pass_order_ids:
        continue
    if not o.get("subscription_pass_id", ""):
        continue
    pm = o.get("payment_method", "") or o.get("payment", "") or "unknown"
    sc = o.get("sales_channel", "") or "unknown"
    payment_methods[pm] += 1
    sales_channels[sc] += 1
    total_by_method[pm] = total_by_method.get(pm, 0) + total

print("=== Sub renewal orders - Payment Method ===")
for k in sorted(total_by_method.keys()):
    print(f"  {k:20s}: {payment_methods[k]:5d} orders  ${total_by_method[k]:>12,.2f}")

print("\n=== Sub renewal orders - Sales Channel ===")
for k, v in sales_channels.most_common():
    print(f"  {k:20s}: {v:5d} orders")

# Check: are "paid_with_pass" orders counted?
paid_with_pass = 0
paid_with_pass_total = 0
for oid, o in orders.items():
    d = o.get("completed_at", "")
    total = money(o.get("total"))
    if not d.startswith("2024-02") or o.get("state", "") not in ("completed", "refunded") or total <= 0:
        continue
    if oid in pass_order_ids:
        continue
    if o.get("paid_with_pass_id", ""):
        paid_with_pass += 1
        paid_with_pass_total += total

print(f"\nOrders paid_with_pass_id (class bookings using pass): {paid_with_pass}, ${paid_with_pass_total:,.2f}")
