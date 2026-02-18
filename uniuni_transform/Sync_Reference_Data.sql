/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
                         Used to associate new reference data with correct carrier
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Idempotent synchronization of reference data tables:
         
         Sync shipping_method table with new service types discovered from bills

Source:   billing.uniuni_bill (for shipping methods)

Targets:  dbo.shipping_method

Note:     Charge types are initialized via Insert_Charge_Types_OneTime.sql
          (one-time setup script, not part of regular pipeline)

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
                 If a wrong bill is processed, the transaction in Insert_ELT_&_CB.sql
                 will rollback, protecting reference data integrity.

Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT;

/*
================================================================================
Block 1: Synchronize Shipping Methods
================================================================================
Discovers distinct service types from uniuni_bill and inserts any new
methods into the shipping_method table. Populates with sensible defaults:
- carrier_id: From @Carrier_id parameter
- method_name: The actual service type from UniUni data
- service_level: Default to 'Standard'
- guaranteed_delivery: Default to 0 (false)
- is_active: Default to 1 (true)
================================================================================
*/

INSERT INTO dbo.shipping_method (
    carrier_id,
    method_name,
    service_level,
    guaranteed_delivery,
    is_active
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    ub.service_type AS method_name,
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active
FROM
    billing.uniuni_bill AS ub
WHERE
    ub.created_date > @lastrun
    AND ub.service_type IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.shipping_method AS sm
        WHERE sm.method_name = ub.service_type
            AND sm.carrier_id = @Carrier_id
    );

SET @ShippingMethodsAdded = @@ROWCOUNT;

-- Return result
SELECT 
    @ShippingMethodsAdded AS ShippingMethodsAdded;