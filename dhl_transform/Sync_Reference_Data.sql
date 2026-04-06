/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Automatically populate and maintain reference tables by discovering
         new values from processed DHL billing data.
         
         Block 1: Sync shipping_method (discovered from data)
         Block 2: Charge types are static -- seeded via migration.sql (commented out)

Source:  billing.dhl_bill + carrier_bill JOIN (file_id filtered)
Targets: dbo.shipping_method

File-Based Filtering: Uses @File_id to process only the current file's data:
         - Joins carrier_bill to filter by file_id

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
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = dhl.carrier_bill_id
WHERE 
    cb.file_id = @File_id
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
Block 2: Synchronize Charge Types (SEEDED VIA MIGRATION)
================================================================================
DHL charge types are static -- all 38 are seeded in migration.sql (Part 5).
This block is kept commented out as a fallback if re-seeding is ever needed.
================================================================================

INSERT INTO dbo.charge_types (
    carrier_id,
    charge_name,
    freight,
    dt,
    charge_category_id
)
SELECT DISTINCT
    @Carrier_id AS carrier_id,
    v.charge_type AS charge_name,
    CASE WHEN v.charge_type = N'Transportation Cost' THEN 1 ELSE 0 END AS freight,
    CASE WHEN v.charge_type IN (N'Gst Tax', N'Hst Tax', N'Pst Tax', N'Vat Tax', N'Duties', N'Other Tax')
         THEN 1 ELSE 0 END AS dt,
    CASE WHEN v.charge_type = N'Transportation Cost' THEN 15
         ELSE 11 END AS charge_category_id
FROM 
    billing.vw_DHLCharges v
WHERE 
    v.file_id = @File_id
    AND v.charge_type IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.charge_name = v.charge_type
            AND ct.carrier_id = @Carrier_id
    );
*/

    SELECT 
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded;

END TRY
BEGIN CATCH
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
    
    THROW 50000, @DetailedError, 1;
END CATCH;
