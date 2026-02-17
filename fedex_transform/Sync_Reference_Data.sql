/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
                         Used to associate new charge types with correct carrier
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
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

Source:  billing.delta_fedex_bill (for service types)
billing.vw_FedExCharges (for charge types)
         
Targets: dbo.shipping_method
dbo.charge_types

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
                 If a wrong bill is processed, the transaction in Insert_ELT_&_CB.sql
                 will rollback, protecting reference data integrity.
      
Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

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
    NULLIF(TRIM(d.[Service Type]), '') AS method_name,
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active
FROM 
billing.delta_fedex_bill d
WHERE 
    NULLIF(TRIM(d.[Service Type]), '') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.shipping_method sm
        WHERE sm.method_name = NULLIF(TRIM(d.[Service Type]), '')
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
    charge_category_id = 16 (FK to dbo.charge_type_category)
- Otherwise:
    charge_category_id = 11 (FK to dbo.charge_type_category)
================================================================================
*/

INSERT INTO dbo.charge_types (
    carrier_id,
    charge_name,
    charge_category_id  -- FK to dbo.charge_type_category.id
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    NULLIF(TRIM(v.charge_type), '') AS charge_name,
    CASE 
        WHEN LOWER(NULLIF(TRIM(v.charge_type), '')) LIKE '%adjustment%' THEN 16  -- FK to charge_type_category
        ELSE 11  -- FK to charge_type_category
    END AS charge_category_id
FROM 
billing.vw_FedExCharges v
WHERE 
    v.created_date > @lastrun
    AND NULLIF(TRIM(v.charge_type), '') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.charge_name = NULLIF(TRIM(v.charge_type), '')
            AND ct.carrier_id = @Carrier_id
    );

SET @ChargeTypesAdded = @@ROWCOUNT;

    -- Return success with row counts for ADF monitoring
    SELECT 
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded, 
        @ChargeTypesAdded AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    -- No transaction to rollback - INSERTs are independently idempotent
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        'FedEx Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;