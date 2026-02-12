/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
                         Used to associate new shipping methods and charge types with correct carrier
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Automatically populate and maintain reference/lookup tables by 
         discovering new values from processed USPS EasyPost billing data.
         
         Syncs shipping_method table with new service types discovered in data.

Source:  test.usps_easy_post_bill (for service types)
         
Targets: test.shipping_method

Note: Charge types for USPS EasyPost are static (5 types) and should be 
      seeded separately during initial setup via manual INSERT.

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
                 This ensures reference data is discovered from validated bills only.
                 If a wrong bill is processed, the transaction in Insert_ELT_&_CB.sql
                 will rollback, protecting reference data integrity.
      
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
    Discovers distinct service types from usps_easypost_bill and inserts any new
    methods into the shipping_method table. Populates with sensible defaults:
    - carrier_id: From @Carrier_id parameter
    - method_name: The actual service type from USPS EasyPost data
    - service_level: Default to 'Standard'
    - guaranteed_delivery: Default to 0 (false)
    - is_active: Default to 1 (true)
    - name_in_bill: NULL (can be updated manually later if needed)
    
    Incremental Processing: Only processes records created after @lastrun
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
        u.service AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active,
        NULL AS name_in_bill
    FROM 
        test.usps_easy_post_bill u
    WHERE 
        u.created_at > @lastrun
        AND u.service IS NOT NULL
        AND NULLIF(TRIM(u.service), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM test.shipping_method sm
            WHERE sm.method_name = u.service
                AND sm.carrier_id = @Carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Return Results
    ================================================================================
    Note: Charge types for USPS EasyPost are static and should be seeded separately 
          via manual INSERT or deployment scripts.
          5 charge types: Base Rate (freight=1), Label Fee, Unknown Charges, 
          Carbon Offset Fee, Insurance Fee
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
        '[USPS EasyPost] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (INSERT independently idempotent)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status, ShippingMethodsAdded
✅ #11 - Charge types are static and seeded separately (not discovered from data)
================================================================================
*/

