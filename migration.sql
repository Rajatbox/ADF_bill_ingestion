/*
================================================================================
Migration: Correctional Invoice Support
================================================================================
Purpose: Allow same invoice to appear in multiple files (original + correctional).
         Replaces unique constraint to include file_id in business key.

Idempotent: DROP uses IF EXISTS pattern where supported; CREATE uses IF NOT EXISTS
            or can be run once. Re-running may require manual handling.
================================================================================*/

SET NOCOUNT ON;
GO

-- 1. Drop existing unique constraint
IF EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'UQ_carrier_bill_number_date_carrier' 
    AND object_id = OBJECT_ID('billing.carrier_bill')
)
BEGIN
    DROP INDEX UQ_carrier_bill_number_date_carrier ON billing.carrier_bill;
END
GO

-- 2. Create new unique index including file_id (for file-based processing)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'UQ_carrier_bill_number_date_carrier_file' 
    AND object_id = OBJECT_ID('billing.carrier_bill')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UQ_carrier_bill_number_date_carrier_file
    ON billing.carrier_bill (bill_number, bill_date, carrier_id, file_id)
    WHERE carrier_id IS NOT NULL AND bill_date IS NOT NULL AND file_id IS NOT NULL;
END
GO
