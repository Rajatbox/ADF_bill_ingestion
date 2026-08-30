# GOFO: Fetching Invoice Number & Bill Date via ADF Dynamic Content

## Why this exists

GOFO's invoice number is **never present in any data row**, regardless of file format version -- it can't be derived from body columns at all.

What *is* stable across every file is row 1 -- a metadata line embedded in the first CSV field, before the real header:

```
Invoice Number：CUS260505001202607002    Invoice Period：07/16/2026-07/31/2026   Invoicing currency：USD
```

(Note: `：` is a **fullwidth colon**, U+FF1A -- not ASCII `:`.)

This gives us the real invoice number and the real invoice period end date directly from the source, instead of fabricating them from body-column date math. `Insert_ELT_&_CB.sql` now takes these as input parameters (`@Invoice_Number`, `@Bill_Date`) rather than computing them.

---

## ADF Pipeline Wiring

### 1. Lookup activity (before the Copy activity)

Add a Lookup activity, `LookupGofoHeader`, against the **same file** as the Copy activity, but pointing at a dataset (or a dataset with activity-level overrides) configured as:
- `skipLineCount: 0`
- `firstRowAsHeader: false`
- Lookup setting: **First row only** = checked

With no header and no skip, ADF auto-names the single populated column `Prop_0` (same convention as `ValidateCarrierInfo.sql`'s `$.Prop_N` usage). The other 30 trailing commas in row 1 produce empty `Prop_1..Prop_30`, which we ignore.

The Copy activity's own dataset keeps `skipLineCount: 2` as before, so `billing.delta_gofo_bill` only ever receives real data rows starting at row 4.

### 2. Dynamic content expressions

Reference: `activity('LookupGofoHeader').output.firstRow.Prop_0`

**Invoice Number:**
```
@split(split(activity('LookupGofoHeader').output.firstRow.Prop_0, '：')[1], ' ')[0]
```
Splitting on the fullwidth colon isolates `CUS260505001202607002    Invoice Period` from the `Invoice Number` label; splitting that on space and taking `[0]` drops the trailing `Invoice Period` text regardless of how many spaces separate them.
→ `CUS260505001202607002`

**Bill Date (invoice period end):**
```
@split(split(split(activity('LookupGofoHeader').output.firstRow.Prop_0, '：')[2], ' ')[0], '-')[1]
```
→ `07/31/2026` (string, `MM/DD/YYYY` -- passed to the Script activity as `nvarchar`; the SQL script converts it with `CONVERT(date, @Bill_Date, 101)`).

### 3. Pass through to the Script activity

Both values feed into the `load_to_elt_stage` Script activity as new parameters alongside `@Carrier_id`/`@File_id`:

| Pipeline Parameter | ADF Expression | SQL Parameter | Type |
|---|---|---|---|
| Invoice Number | expression above | `@Invoice_Number` | String / nvarchar(50) |
| Bill Date | expression above | `@Bill_Date` | String / nvarchar(10), converted with style 101 in-script |

---

## Verifying by hand (Python, for sanity-checking against real files before wiring the pipeline)

```python
def inspect_gofo_header(raw):
    colon_parts = raw.split("：")
    invoice_number = colon_parts[1].split(" ")[0] if len(colon_parts) >= 2 else None
    period = colon_parts[2].split(" ")[0] if len(colon_parts) >= 3 else None
    period_end = period.split("-")[1] if period and "-" in period else None
    return invoice_number, period_end

with open("some_gofo_bill.csv", encoding="utf-8-sig") as f:
    raw = f.readline().split(",")[0]
print(inspect_gofo_header(raw))
```

---

## Note on `A-scan Date`

An older GOFO file format lacked an `A-scan Date` column. Per the client, the current/latest format includes it, and that's the format being built against going forward. `A-scan Date` is part of `billing.delta_gofo_bill`/`billing.gofo_bill` and drives `shipment_attributes.shipment_date` -- see `Insert_Unified_tables.sql`. This is unrelated to the invoice-number/bill-date fetch above, which is needed regardless of file format since the invoice number was never in any data row to begin with.
