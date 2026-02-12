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

Purpose: Automatically populate and maintain the shipping_method reference table
         by discovering new values from processed DHL billing data.

         Charge types are NOT synced here — DHL is a wide deterministic format
         with 4 fixed charge columns. Charge types should be seeded once.

Source:  billing.dhl_bill (for shipping methods - discovered from data)
Target:  dbo.shipping_method

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
Synchronize Shipping Methods
================================================================================
Discovers distinct shipping methods from dhl_bill and inserts any new
methods into the shipping_method table. Populates with sensible defaults:
- carrier_id: From @Carrier_id parameter
- method_name: The actual shipping method from DHL data
- service_level: Default to 'Standard'
- guaranteed_delivery: Default to 0 (false)
- is_active: Default to 1 (true)
- name_in_bill: NULL (can be updated manually later if needed)

Examples: 'DHL Parcel International Standard', 'DHL Parcel International Direct'
================================================================================
*/

INSERT INTO dbo.shipping_method (
    carrier_id,
    method_name,
    service_level,
    guaranteed_delivery,
    is_active,
    name_in_bill
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    CAST(dhl.shipping_method AS varchar(255)) AS method_name,
    'Standard' AS service_level,
    0 AS guaranteed_delivery,
    1 AS is_active,
    NULL AS name_in_bill
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
