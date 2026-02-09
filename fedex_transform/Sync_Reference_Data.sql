/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
                         Used to associate new charge types with correct carrier
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ChargeTypesAdded: INT - Number of new charge types discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Automatically populate and maintain reference/lookup tables by 
         discovering new values from processed FedEx billing data.
         
         Block 1: Sync shipping_method table with new service types
         Block 2: Sync charge_types table with new charge descriptions

Source:  test.delta_fedex_bill (for service types)
Test.vw_FedExCharges (for charge types)
         
Targets: test.shipping_method
test.charge_types

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
                 If a wrong bill is processed, the transaction in Insert_ELT_&_CB.sql
                 will rollback, protecting reference data integrity.
      
Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

/*
================================================================================
Block 1: Synchronize Shipping Methods
================================================================================
Discovers distinct service types from delta_fedex_bill and inserts any new
methods into the shipping_method table. Populates with sensible defaults:
- carrier_id: From @Carrier_id parameter
- method_name: The actual service type from FedEx data
- service_level: Default to 'Standard'
- guaranteed_delivery: Default to 0 (false)
- is_active: Default to 1 (true)
- name_in_bill: NULL (can be updated manually later if needed)
================================================================================
*/

INSERT INTO test.shipping_method (
    carrier_id,
    method_name,
    service_level,
    guaranteed_delivery,
    is_active,
    name_in_bill
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    CAST(d.[Service Type] AS varchar(255)) AS method_name,
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active,
    NULL AS name_in_bill
FROM 
test.delta_fedex_bill d
WHERE 
    d.[Service Type] IS NOT NULL
    AND NULLIF(TRIM(CAST(d.[Service Type] AS varchar)), '') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM test.shipping_method sm
        WHERE sm.method_name = CAST(d.[Service Type] AS varchar(255))
            AND sm.carrier_id = @Carrier_id
    );

SET @ShippingMethodsAdded = @@ROWCOUNT;

/*
================================================================================
Block 2: Synchronize Charge Types
================================================================================
Discovers distinct charge types from vw_FedExCharges and inserts any new
charge types into the charge_types table. Applies category logic:
- If charge_type contains 'adjustment' (case insensitive):
    category = 'Adjustment', charge_category_id = 16
- Otherwise:
    category = 'Other', charge_category_id = 11
================================================================================
*/

INSERT INTO test.charge_types (
    carrier_id,
    charge_name,
    category,
    charge_category_id
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    CAST(v.charge_type AS varchar(255)) AS charge_name,
    CASE 
        WHEN LOWER(v.charge_type) LIKE '%adjustment%' THEN 'Adjustment'
        ELSE 'Other'
    END AS category,
    CASE 
        WHEN LOWER(v.charge_type) LIKE '%adjustment%' THEN 16
        ELSE 11
    END AS charge_category_id
FROM 
Test.vw_FedExCharges v
WHERE 
    v.created_date > @lastrun
    AND v.charge_type IS NOT NULL
    AND NULLIF(TRIM(v.charge_type), '') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM test.charge_types ct
        WHERE ct.charge_name = CAST(v.charge_type AS varchar(255))
            AND ct.carrier_id = @Carrier_id
    );

SET @ChargeTypesAdded = @@ROWCOUNT;

-- Final single result set for the ADF Debug Window
SELECT 
    @ShippingMethodsAdded AS ShippingMethodsAdded, 
    @ChargeTypesAdded AS ChargeTypesAdded;