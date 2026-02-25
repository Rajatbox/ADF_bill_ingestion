/*
================================================================================
Migration Script: FlavorCloud Aggregator Integration
================================================================================
Purpose: Standardize FlavorCloud carrier column naming to match other aggregators

Changes:
  1. Rename carrier_name → integrated_carrier in billing.flavorcloud_bill
  2. Add integrated_carrier_id to billing.shipment_attributes

================================================================================
*/

SET NOCOUNT ON;
GO

-- Step 1: Rename carrier_name to integrated_carrier in FlavorCloud bill table
EXEC sp_rename 'billing.flavorcloud_bill.carrier_name', 'integrated_carrier', 'COLUMN';
GO

-- Step 2: Add integrated_carrier_id to shipment_attributes
ALTER TABLE billing.shipment_attributes
ADD integrated_carrier_id INT NULL;
GO

PRINT '✓ Migration completed successfully!';
GO
