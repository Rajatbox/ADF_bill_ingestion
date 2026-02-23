/*
================================================================================
Migration Script: Aggregator Integration
================================================================================
Purpose: Add support for hierarchical carrier model (aggregator → actual carrier)

Changes:
  1. Add is_aggregator to dbo.carrier
  2. Add integrated_carrier to billing.usps_easy_post_bill
  3. Add integrated_carrier to billing.eliteworks_bill
  4. Add integrated_carrier_id to dbo.shipping_method
  5. Rename delta_usps_easypost_bill → delta_easypost_bill
  6. Rename usps_easy_post_bill → easypost_bill

================================================================================
*/

SET NOCOUNT ON;
GO




-- Step 4: Add integrated_carrier_id to billing.shipment_attributes
ALTER TABLE billing.shipment_attributes
ADD integrated_carrier_id INT NULL;
GO

-- Step 6: Rename delta_usps_easypost_bill to delta_easypost_bill
EXEC sp_rename 'billing.delta_usps_easypost_bill', 'delta_easypost_bill';
GO

-- Step 7: Rename usps_easy_post_bill to easypost_bill
EXEC sp_rename 'billing.usps_easy_post_bill', 'easypost_bill';
GO

