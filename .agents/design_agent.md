# Design Agent

## Role
Transform script designer and generator

## Responsibilities
1. Read carrier CSV and stored procedure
2. Apply design constraints
3. Generate 3 transform scripts
4. Provide validation test

## Prerequisites (from Setup Agent)
- ✅ `[carrier]_example_bill.csv` exists and populated
- ✅ `reference_stored_procedure.sql` exists and populated
- ✅ `additional_reference.md` exists (optional - may contain extra context, helper queries, views, or business rules)

## Workflow

### Step 1: Read Files
```
1. Read [carrier]_example_bill.csv
   - Extract column names
   - Identify format (narrow vs wide)
   - Note charge structure

2. Read reference_stored_procedure.sql
   - Extract business logic
   - Note multi-piece handling (if any)
   - Note correction logic
   - Identify unit conversions

3. Read additional_reference.md (if populated)
   - Extract any supplementary business logic, views, helper queries
   - Cross-reference with stored procedure for completeness
   - Note any edge cases or special handling mentioned
```

### Step 1.5: Create Plan & Ask Questions (MANDATORY)
**Before writing any scripts, you MUST:**

1. **Present an implementation plan** to the user summarizing:
   - CSV format identified (narrow vs wide, key columns)
   - Business logic extracted from stored procedure and additional reference
   - Proposed delta table schema (column list with types)
   - Proposed charge mapping strategy
   - Unit conversion approach (weight → OZ, dimensions → IN)
   - Any assumptions being made

2. **Ask clarifying questions** about anything unclear:
   - Ambiguous column mappings
   - Missing business rules
   - Charge type categorization
   - Multi-piece / correction / void handling
   - Any carrier-specific edge cases

3. **STOP and wait for user approval** before proceeding to script generation
   - Do NOT generate scripts until the user confirms the plan
   - Incorporate any feedback from the user into the plan

### Step 2: Generate Delta Table
```sql
-- Create staging table with 1:1 column mapping
-- Note: Database name is parameterized per environment (DEV/UAT/PROD)
CREATE TABLE test.delta_[carrier]_bill (
    [Column1 from CSV] VARCHAR(255) NULL,
    [Column2 from CSV] VARCHAR(255) NULL,
    -- ... all columns as VARCHAR
);
```

### Step 3: Read Design Constraints
**MUST READ:** `DESIGN_CONSTRAINTS.md`

Key rules to apply:
- Transaction boundaries (where to use BEGIN TRAN)
- Fail-fast CAST (not TRY_CAST)
- Idempotency pattern (NOT EXISTS with carrier_id)
- Unit conversions (OZ, IN)
- Cost calculation (view, not stored)

### Step 4: Generate Scripts

**Input Parameters (All Scripts):**
- `@Carrier_id` (INT) - Carrier identifier from parent pipeline
- `@File_id` (INT) - File tracking ID from ValidateCarrierInfo.sql

**No longer used**: `@lastrun` (replaced by file-based tracking)

---

#### 1. Insert_ELT_&_CB.sql
Template:
```sql
BEGIN TRANSACTION;
    -- Step 1: INSERT carrier_bill (with file_id)
    INSERT INTO carrier_bill (
        carrier_id, bill_number, bill_date, 
        total_amount, num_shipments, account_number, 
        file_id  -- NEW
    )
    SELECT ...., @File_id AS file_id
    WHERE NOT EXISTS (
        SELECT 1 FROM carrier_bill cb
        WHERE cb.file_id = @File_id  -- File-based idempotency
    );
    
    -- Step 2: INSERT [carrier]_bill
    INSERT INTO [carrier]_bill ...
    WHERE NOT EXISTS (...);
COMMIT TRANSACTION;
```

**Rules:**
- ✅ Wrapped in transaction
- ✅ Add `file_id` column to carrier_bill INSERT (value: `@File_id`)
- ✅ Idempotency: `WHERE cb.file_id = @File_id` (checks if file already processed)
- ✅ Direct CAST (fail fast)
- ✅ Returns: Status, InvoicesInserted, LineItemsInserted

#### 2. Sync_Reference_Data.sql
Template:
```sql
-- No transaction

-- Step 1: Sync shipping methods
INSERT INTO shipping_method ...
FROM [carrier]_bill [carrier]
JOIN carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
WHERE cb.file_id = @File_id  -- File-based filtering
  AND NOT EXISTS (method_name, carrier_id);

-- Step 2: Sync charge types
INSERT INTO charge_types ...
FROM [carrier]_bill [carrier]
JOIN carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
WHERE cb.file_id = @File_id  -- File-based filtering
  AND NOT EXISTS (charge_name, carrier_id);
```

**Rules:**
- ✅ No transaction (idempotent)
- ✅ Filter by `@File_id` (not `@lastrun`)
- ✅ Join to carrier_bill: `WHERE cb.file_id = @File_id`
- ✅ NOT EXISTS includes carrier_id
- ✅ Returns: ShippingMethodsAdded, ChargeTypesAdded

#### 3. Insert_Unified_tables.sql
Template:
```sql
-- No transaction
-- Note: Database name is parameterized per environment

-- Part 1: INSERT shipment_attributes
INSERT INTO shipment_attributes (
    carrier_id,
    tracking_number,
    billed_weight_oz,    -- MUST convert to OZ
    billed_length_in,    -- MUST convert to IN
    ...
)
SELECT
    @Carrier_id,
    tracking_number,
    CASE WHEN unit='LB' THEN weight*16 ELSE weight END,  -- Convert
    CASE WHEN unit='CM' THEN length/2.54 ELSE length END, -- Convert
    ...
FROM [carrier]_bill [carrier]
JOIN carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
WHERE cb.file_id = @File_id  -- File-based filtering
  AND NOT EXISTS (carrier_id, tracking_number);

-- Part 2: INSERT shipment_charges
INSERT INTO shipment_charges ...
FROM [carrier]_bill [carrier]
JOIN carrier_bill cb ON cb.carrier_bill_id = [carrier].carrier_bill_id
WHERE cb.file_id = @File_id  -- File-based filtering
  AND NOT EXISTS (shipment_attribute_id, carrier_bill_id, charge_type_id);
```

**Rules:**
- ✅ No transaction (idempotent)
- ✅ Filter by `@File_id` (not `@lastrun`)
- ✅ Part 1: Join carrier_bill, filter `WHERE cb.file_id = @File_id`
- ✅ Part 2: Join carrier_bill (or use view with file_id), filter `WHERE cb.file_id = @File_id`
- ✅ Unit conversions applied
- ✅ NOT EXISTS on business keys
- ✅ Returns: Status, AttributesInserted, ChargesInserted

### Step 5: Provide Validation Test

**ONE test query:**
```sql
-- Test: File total vs charges total
DECLARE @File_id INT = /* test file_id */;
DECLARE @Carrier_id INT = /* carrier_id */;

WITH file_total AS (
    SELECT SUM(cb.total_amount) AS expected
    FROM carrier_bill cb
    WHERE cb.file_id = @File_id  -- File-based filtering
),
charges_total AS (
    SELECT SUM(sc.amount) AS actual
    FROM shipment_charges sc
    JOIN carrier_bill cb ON cb.carrier_bill_id = sc.carrier_bill_id
    WHERE cb.file_id = @File_id  -- File-based filtering
)
SELECT 
    expected,
    actual,
    ABS(expected - actual) AS difference,
    CASE WHEN ABS(expected - actual) < 0.01 
         THEN '✅ PASS' 
         ELSE '❌ FAIL' 
    END AS result
FROM file_total, charges_total;
```

## Checklist Before Delivery

- [ ] Read carrier CSV
- [ ] Read stored procedure
- [ ] Read additional_reference.md (if populated)
- [ ] Read DESIGN_CONSTRAINTS.md
- [ ] Presented implementation plan to user
- [ ] Asked clarifying questions
- [ ] Received user approval to proceed
- [ ] Generated delta table schema
- [ ] Generated Insert_ELT_&_CB.sql (with transaction)
- [ ] Generated Sync_Reference_Data.sql (no transaction)
- [ ] Generated Insert_Unified_tables.sql (with unit conversions)
- [ ] Provided 1 validation test
- [ ] Applied all design constraints

## What NOT to do
- ❌ Don't modify parent_pipeline files
- ❌ Don't create README or extra docs
- ❌ Don't use TRY_CAST (use CAST)
- ❌ Don't store cost in shipment_attributes
- ❌ Don't forget unit conversions (OZ, IN)
- ❌ Don't forget carrier_id in NOT EXISTS

## What NOT to do (Plan Phase)
- ❌ Don't generate scripts before presenting the plan
- ❌ Don't skip asking clarifying questions
- ❌ Don't proceed without user approval of the plan

## What TO do
- ✅ Read all input files (CSV, stored procedure, additional reference)
- ✅ Read design constraints
- ✅ Create implementation plan and present to user FIRST
- ✅ Ask clarifying questions before generating scripts
- ✅ Wait for user approval before proceeding
- ✅ Generate exactly 3 scripts
- ✅ Apply all design rules
- ✅ Provide exactly 1 test query
- ✅ Preserve business logic from stored procedure and additional reference

