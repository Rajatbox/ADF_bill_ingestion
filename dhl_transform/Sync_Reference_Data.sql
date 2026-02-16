/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Automatically populate and maintain reference tables by discovering
         new values from processed DHL billing data.
         
         Block 1: Sync shipping_method (discovered from data)
         Block 2: ONE-TIME SEED of 4 fixed DHL charge types (static)

Source:  billing.dhl_bill (for shipping methods - discovered from data)
Targets: dbo.shipping_method
         dbo.charge_types (one-time seed of 4 fixed charges)

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
      
Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT;

BEGIN TRY

/*
================================================================================
Block 1: Synchronize Shipping Methods
================================================================================
Discovers distinct shipping methods from dhl_bill and inserts any new
methods into the shipping_method table. Populates with sensible defaults:
- carrier_id: From @Carrier_id parameter
- method_name: The actual shipping method from DHL data
- service_level: Default to 'Standard'
- guaranteed_delivery: Default to 0 (false)
- is_active: Default to 1 (true)

Examples: 'DHL Parcel International Standard', 'DHL Parcel International Direct'
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
    CAST(dhl.shipping_method AS varchar(255)) AS method_name,
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active
FROM 
    billing.dhl_bill dhl
WHERE 
    dhl.created_date > @lastrun
    AND dhl.shipping_method IS NOT NULL
    AND NULLIF(TRIM(CAST(dhl.shipping_method AS varchar(255))), '') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.shipping_method sm
        WHERE sm.method_name = CAST(dhl.shipping_method AS varchar(255))
            AND sm.carrier_id = @Carrier_id
    );

SET @ShippingMethodsAdded = @@ROWCOUNT;

/*
================================================================================
ONE-TIME SEED: Synchronize Charge Types (DHL Fixed Charges)
================================================================================
DHL has a wide deterministic format with 4 fixed charge columns:
- Transportation Cost (freight)
- Non-Qualified Dim
- Fuel Surcharge
- Delivery Area Surcharge

This block seeds these static charge types if they don't exist.
After first run, this will no-op due to NOT EXISTS check.

Note: All DHL charge types default to charge_category_id = 11 (Other)
================================================================================


INSERT INTO dbo.charge_types (
    carrier_id,
    charge_name,
    freight,
    charge_category_id  -- FK to dbo.charge_type_category.id
)
SELECT charge_data.carrier_id, charge_data.charge_name, charge_data.freight, charge_data.charge_category_id
FROM (
    VALUES 
        (@Carrier_id, 'Transportation Cost', 1, 15),  -- Freight charge
        (@Carrier_id, 'Non-Qualified Dim', 0, 12),
        (@Carrier_id, 'Fuel Surcharge', 0, 8),
        (@Carrier_id, 'Delivery Area Surcharge', 0, 4)
) AS charge_data(carrier_id, charge_name, freight, charge_category_id)
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.charge_types ct
    WHERE ct.charge_name = charge_data.charge_name
        AND ct.carrier_id = @Carrier_id
);
*/
    -- Return success with row counts for ADF monitoring
    SELECT 
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded;

END TRY
BEGIN CATCH
    -- No transaction to rollback - INSERT is independently idempotent
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        'DHL Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
