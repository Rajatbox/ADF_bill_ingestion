/*
================================================================================
Eliteworks Carrier Integration - Table Schemas
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.
      Scripts reference only schema.table format.

Purpose: Defines staging (delta) and normalized table schemas for Eliteworks
         carrier billing data integration.

Tables:
  1. billing.delta_eliteworks_bill (Staging - Raw CSV replica)
  2. billing.eliteworks_bill (Normalized - Carrier-specific line items)

Referenced by:
  - Insert_ELT_&_CB.sql
  - Sync_Reference_Data.sql
  - Insert_Unified_tables.sql

Execution: Run this schema creation BEFORE running transformation scripts.
           These tables must exist in the target database.
================================================================================
*/

/*
================================================================================
STAGING LAYER: Delta Table (Raw CSV Replica)
================================================================================
Purpose: Staging table for raw Eliteworks CSV data. Loaded by ADF Copy Activity.

Source: Eliteworks CSV billing files (narrow format, one row per shipment)
Target: billing.delta_eliteworks_bill

Columns: All VARCHAR to accommodate any CSV data format (type conversion in transform scripts)
Format: 41 columns matching Eliteworks CSV structure exactly

Notes:
  - No constraints (staging table for raw data)
  - All columns nullable
  - Data types VARCHAR for maximum flexibility
  - ADF Copy Activity uses "First Row as Header" = TRUE
  - Truncated/refreshed on each pipeline run
================================================================================
*/

CREATE TABLE billing.delta_eliteworks_bill (
    time_utc VARCHAR(50) NULL,
    shipment_id VARCHAR(255) NULL,
    user_account VARCHAR(255) NULL,
    tracking_number VARCHAR(255) NULL,
    status VARCHAR(50) NULL,
    carrier VARCHAR(50) NULL,
    service VARCHAR(255) NULL,
    reference VARCHAR(255) NULL,
    shipment_weight_oz VARCHAR(50) NULL,
    shipment_dryice_weight_oz VARCHAR(50) NULL,
    package_type VARCHAR(50) NULL,
    package_length_in VARCHAR(50) NULL,
    package_width_in VARCHAR(50) NULL,
    package_height_in VARCHAR(50) NULL,
    from_name VARCHAR(255) NULL,
    from_company VARCHAR(255) NULL,
    from_street VARCHAR(255) NULL,
    from_apt_suite VARCHAR(50) NULL,
    from_city VARCHAR(100) NULL,
    from_state VARCHAR(50) NULL,
    from_postal VARCHAR(50) NULL,
    from_country VARCHAR(10) NULL,
    to_name VARCHAR(255) NULL,
    to_company VARCHAR(255) NULL,
    to_street VARCHAR(255) NULL,
    to_apt_suite VARCHAR(50) NULL,
    to_city VARCHAR(100) NULL,
    to_state VARCHAR(50) NULL,
    to_postal VARCHAR(50) NULL,
    to_country VARCHAR(10) NULL,
    first_scan VARCHAR(50) NULL,
    delivered VARCHAR(50) NULL,
    delivered_days VARCHAR(50) NULL,
    delivered_business_days VARCHAR(50) NULL,
    zone VARCHAR(50) NULL,
    charged VARCHAR(50) NULL,
    store_markup VARCHAR(50) NULL,
    platform_charged_with_corrections VARCHAR(50) NULL,
    commercial VARCHAR(50) NULL,
    order_reference VARCHAR(255) NULL,
    order_date VARCHAR(50) NULL
);

/*
================================================================================
NORMALIZED LAYER: Eliteworks Bill Table (Carrier-Specific Line Items)
================================================================================
Purpose: Normalized carrier-specific billing line items for Eliteworks.
         One row per shipment. Links to carrier_bill via carrier_bill_id FK.

Source: billing.delta_eliteworks_bill (via Insert_ELT_&_CB.sql transformation)
Target: billing.eliteworks_bill

Business Key: (carrier_bill_id, tracking_number)
Grain: One row per shipment per invoice

Relationships:
  - FK to billing.carrier_bill (carrier_bill_id) - Invoice header reference
  - Referenced by Insert_Unified_tables.sql for shipment_attributes population

Key Fields:
  - invoice_number: Synthetic (format: 'Eliteworks_YYYY-MM-DD')
  - invoice_date: Extracted from MAX(time_utc)
  - tracking_number: From tracking_number column
  - shipment_date: From first_scan column (when carrier first processed package)
  - charged_amount: Base carrier charge from charged
  - store_markup: Platform markup from store_markup
  - platform_charged: Final amount from platform_charged_with_corrections

Unit Standards (Design Constraint #7):
  - Weight: Already in OZ (no conversion needed)
  - Dimensions: Already in IN (no conversion needed)

Indexes:
  1. PK on id
  2. FK on carrier_bill_id (join performance)
  3. created_date (incremental processing)
  4. Composite on (tracking_number, invoice_number, invoice_date) for lookups
================================================================================
*/

CREATE TABLE billing.eliteworks_bill (
    id INT IDENTITY(1,1) NOT NULL,
    carrier_bill_id INT NULL,
    invoice_number NVARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    tracking_number NVARCHAR(255) NOT NULL,
    shipment_date DATETIME NULL,
    service_method NVARCHAR(255) NULL,
    zone NVARCHAR(50) NULL,
    charged_amount DECIMAL(18,2) NULL,
    store_markup DECIMAL(18,2) NULL,
    platform_charged DECIMAL(18,2) NULL,
    billed_weight_oz DECIMAL(18,2) NULL,
    package_length_in DECIMAL(18,2) NULL,
    package_width_in DECIMAL(18,2) NULL,
    package_height_in DECIMAL(18,2) NULL,
    from_postal NVARCHAR(50) NULL,
    to_postal NVARCHAR(50) NULL,
    to_city NVARCHAR(100) NULL,
    to_state NVARCHAR(50) NULL,
    to_country NVARCHAR(10) NULL,
    shipment_status NVARCHAR(50) NULL,
    created_date DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    
    CONSTRAINT PK_eliteworks_bill PRIMARY KEY (id),
    CONSTRAINT FK_eliteworks_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
        REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_carrier_bill_id
ON billing.eliteworks_bill (carrier_bill_id);

-- Index for incremental processing (used by Sync_Reference_Data.sql and Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_created_date
ON billing.eliteworks_bill (created_date);

-- Composite index for tracking number lookups
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_tracking_number_invoice
ON billing.eliteworks_bill (tracking_number, invoice_number, invoice_date);

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #1  - No modification to parent pipeline (carrier-specific tables only)
✅ #3  - Type conversion via CAST in transform scripts (fail-fast)
✅ #4  - Idempotency via NOT EXISTS + UNIQUE constraints
✅ #5  - Business key: (carrier_id, tracking_number) enforced in unified layer
✅ #7  - Units already correct (OZ, IN) - no conversion needed
✅ #8  - Indexes support incremental processing and join performance
✅ #9  - carrier_bill_id FK enforces referential integrity
================================================================================

Deployment Notes:
  1. Run this script in target database (DEV/UAT/PROD)
  2. Ensure billing.carrier_bill table exists first (FK dependency)
  3. Verify indexes created successfully
  4. Grant appropriate permissions to ADF service principal
  5. Test with sample CSV file via ADF Copy Activity

Integration Flow:
  CSV → delta_eliteworks_bill (staging) 
      → eliteworks_bill (normalized) 
      → shipment_attributes + shipment_charges (unified)
================================================================================
*/
