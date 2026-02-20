/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
                         Used to associate new shipping methods with correct carrier
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Automatically populate and maintain reference/lookup tables by 
         discovering new values from processed FlavorCloud billing data.
         
         Discovers and syncs shipping methods from service_level column 
         in flavorcloud_bill (e.g., "STANDARD", "EXPRESS").

Source:  billing.flavorcloud_bill + carrier_bill JOIN (file_id filtered)
         
Targets: dbo.shipping_method

File-Based Filtering: Uses @File_id to process only the current file's data:
         - Joins carrier_bill to filter by file_id

Note:    Charge types should be seeded separately using Insert_Charge_Types.sql
         before running the main pipeline.

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
      
Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
No Transaction: Each INSERT is independently idempotent via unique constraints
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Block 1: Synchronize Shipping Methods
    ================================================================================
    Discovers distinct service levels from flavorcloud_bill and inserts any new 
    methods into the shipping_method table.
    
    Populates with sensible defaults:
    - carrier_id: From @Carrier_id parameter
    - method_name: The actual service level from FlavorCloud data (e.g., "STANDARD", "EXPRESS")
    - service_level: Default to 'Standard'
    - guaranteed_delivery: Default to 0 (false)
    - is_active: Default to 1 (true)
    
    File-Based Filtering: Joins carrier_bill and filters by file_id
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
        f.service_level AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active
    FROM 
        billing.flavorcloud_bill f
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = f.carrier_bill_id
    WHERE 
        cb.file_id = @File_id  -- FILE-BASED FILTERING
        AND f.service_level IS NOT NULL
        AND NULLIF(TRIM(f.service_level), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method sm
            WHERE sm.method_name = f.service_level
                AND sm.carrier_id = @Carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Return Results
    ================================================================================
    */
    SELECT 
        'SUCCESS' AS Status,
        @ShippingMethodsAdded AS ShippingMethodsAdded;

END TRY
BEGIN CATCH
    /*
    ================================================================================
    Error Handling: Return Detailed Error Information
    ================================================================================
    Note: No transaction to rollback - each INSERT is independently idempotent
    ================================================================================
    */
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        '[FlavorCloud] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (INSERT independently idempotent)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status, ShippingMethodsAdded

Note: Charge types should be seeded using Insert_Charge_Types.sql before running
      the main pipeline. This ensures charge types exist before Insert_Unified_tables.sql
      attempts to map charges.
================================================================================
*/
