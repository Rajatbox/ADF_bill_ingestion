/*
================================================================================
Migration: Add shipper_company, shipper_name, original_ref2 to fedex_bill
           Add shipment_reference_1, shipment_reference_2 to ups_bill
================================================================================
Purpose: Pass through columns already present in the delta staging tables
         into their respective silver (line-item) tables so they are
         retained on the line-item records.
           - fedex_bill: [Shipper Company], [Shipper Name], [Original Ref#2]
           - ups_bill:   [Shipment Reference Number 1], [Shipment Reference Number 2]

Affected scripts (updated in this release):
  - fedex_transform/Insert_ELT_&_CB.sql (maps bronze → silver)
  - ups_transform/Insert_ELT_&_CB.sql   (maps bronze → silver)

Idempotent: Safe to run multiple times.
================================================================================
*/

SET NOCOUNT ON;

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.fedex_bill')
      AND name = 'shipper_company'
)
BEGIN
    ALTER TABLE billing.fedex_bill
    ADD shipper_company nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;

    PRINT 'Added shipper_company to billing.fedex_bill';
END
ELSE
BEGIN
    PRINT 'Column shipper_company already exists on billing.fedex_bill — skipped';
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.fedex_bill')
      AND name = 'shipper_name'
)
BEGIN
    ALTER TABLE billing.fedex_bill
    ADD shipper_name nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;

    PRINT 'Added shipper_name to billing.fedex_bill';
END
ELSE
BEGIN
    PRINT 'Column shipper_name already exists on billing.fedex_bill — skipped';
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.fedex_bill')
      AND name = 'original_ref2'
)
BEGIN
    ALTER TABLE billing.fedex_bill
    ADD original_ref2 nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;

    PRINT 'Added original_ref2 to billing.fedex_bill';
END
ELSE
BEGIN
    PRINT 'Column original_ref2 already exists on billing.fedex_bill — skipped';
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.ups_bill')
      AND name = 'shipment_reference_1'
)
BEGIN
    ALTER TABLE billing.ups_bill
    ADD shipment_reference_1 nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;

    PRINT 'Added shipment_reference_1 to billing.ups_bill';
END
ELSE
BEGIN
    PRINT 'Column shipment_reference_1 already exists on billing.ups_bill — skipped';
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.ups_bill')
      AND name = 'shipment_reference_2'
)
BEGIN
    ALTER TABLE billing.ups_bill
    ADD shipment_reference_2 nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL;

    PRINT 'Added shipment_reference_2 to billing.ups_bill';
END
ELSE
BEGIN
    PRINT 'Column shipment_reference_2 already exists on billing.ups_bill — skipped';
END

PRINT 'Migration complete.';
