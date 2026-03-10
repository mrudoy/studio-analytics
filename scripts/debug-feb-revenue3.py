#!/usr/bin/env python3
"""Compare computed revenue by category vs admin report for Feb 2024."""
import csv, os

DIR = os.path.expanduser("~/Downloads/union_data_export-sky-ting-20230830-20260223-zFoHME8GZ2ESfu8LzvDe27NU")

def money(v):
    return float((v or "0").replace("$", "").replace(",", ""))

# Load lookup tables
pass_types = {}
with open(f"{DIR}/pass_types.csv") as f:
    for r in csv.DictReader(f):
        pass_types[r["id"]] = r

rev_cats = {}
with open(f"{DIR}/revenue_categories.csv") as f:
    for r in csv.DictReader(f):
        rev_cats[r["id"]] = r.get("name", "")

orders = {}
with open(f"{DIR}/orders.csv") as f:
    for r in csv.DictReader(f):
        orders[r["id"]] = r

passes = {}
with open(f"{DIR}/passes.csv") as f:
    for r in csv.DictReader(f):
        passes[r["id"]] = r

events = {}
with open(f"{DIR}/events.csv") as f:
    for r in csv.DictReader(f):
        events[r["id"]] = r

# Resolve category for a pass
def resolve_pass_category(p):
    pt_id = p.get("pass_type_id", "")
    if pt_id and pt_id in pass_types:
        rc_id = pass_types[pt_id].get("revenue_category_id", "")
        if rc_id and rc_id in rev_cats:
            return rev_cats[rc_id]
    return None

# Step 2: Non-sub pass revenue by category
computed = {}
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
        rc = resolve_pass_category(p) or "Uncategorized"
        computed[rc] = computed.get(rc, 0) + total

# Step 3: Non-pass order revenue by category
for oid, o in orders.items():
    d = o.get("completed_at", "")
    total = money(o.get("total"))
    if not d.startswith("2024-02") or o.get("state", "") not in ("completed", "refunded") or total <= 0:
        continue
    if oid in pass_order_ids:
        continue

    rc = None
    sub_pass_id = o.get("subscription_pass_id", "")
    if sub_pass_id and sub_pass_id in passes:
        rc = resolve_pass_category(passes[sub_pass_id])
    if not rc:
        event_id = o.get("event_id", "")
        if event_id and event_id in events:
            rc_id = events[event_id].get("revenue_category_id", "")
            if rc_id and rc_id in rev_cats:
                rc = rev_cats[rc_id]
    rc = rc or "Uncategorized"
    computed[rc] = computed.get(rc, 0) + total

# Admin report
admin = {}
with open(os.path.expanduser("~/Downloads/union-revenue-categories-sky-ting-20260309-1853.csv")) as f:
    for r in csv.DictReader(f):
        admin[r["revenue_category"]] = money(r.get("revenue"))

# Compare
all_cats = sorted(set(list(computed.keys()) + list(admin.keys())))
print(f"{'Category':40s} {'Computed':>12s} {'Admin':>12s} {'Diff':>12s}")
print("-" * 80)
total_comp = 0
total_admin = 0
for cat in all_cats:
    c = computed.get(cat, 0)
    a = admin.get(cat, 0)
    total_comp += c
    total_admin += a
    diff = c - a
    flag = " <<" if abs(diff) > 100 else ""
    print(f"{cat:40s} ${c:>11,.2f} ${a:>11,.2f} ${diff:>11,.2f}{flag}")
print("-" * 80)
print(f"{'TOTAL':40s} ${total_comp:>11,.2f} ${total_admin:>11,.2f} ${total_comp - total_admin:>11,.2f}")
