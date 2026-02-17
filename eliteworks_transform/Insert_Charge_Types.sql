/*
================================================================================
ONE-TIME SEED Script: Charge Types for Eliteworks
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ChargeTypesAdded: INT - Number of charge types added
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: ONE-TIME seed of Eliteworks charge types into dbo.charge_types.
         This should be run once during initial setup before the main pipeline.
         After first successful run, this script will no-op (NOT EXISTS prevents duplicates).

Charge Types (Option 1 - Carrier Cost Focus):
- Base Rate (freight=1) - The base carrier charge ([Charged] column)
- Store Markup - Platform markup charge ([Store Markup] column)
- Platform Charged - Final billed amount ([Platform Charged] column)

Charge Category Mapping (Design Constraint #11):
- Base Rate → charge_category_id = 11 (Other)
- Store Markup → charge_category_id = 11 (Other)
- Platform Charged → charge_category_id = 11 (Other)

Note: No carrier prefix in charge names (consistent with USPS EasyPost pattern)

Execution: Run once during carrier setup, before any billing data processing.
           Can be safely rerun (idempotent via NOT EXISTS check).

Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Insert Charge Types (Eliteworks Fixed Charges)
    ================================================================================
    Seeds 3 static charge types for Eliteworks carrier.
    
    After first run, this will no-op due to NOT EXISTS check.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        charge_category_id  -- FK to dbo.charge_type_category.id
    )
    SELECT charge_data.carrier_id, charge_data.charge_name, charge_data.freight, charge_data.charge_category_id
    FROM (
        VALUES 
            (@Carrier_id, 'Base Rate', 1, 11),         -- Freight charge (Other category)
            (@Carrier_id, 'Store Markup', 0, 11),      -- Markup charge (Other category)
            (@Carrier_id, 'Platform Charged', 0, 11)   -- Final billed amount (Other category)
    ) AS charge_data(carrier_id, charge_name, freight, charge_category_id)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.charge_name = charge_data.charge_name
            AND ct.carrier_id = @Carrier_id
    );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Return Results
    ================================================================================
    */
    SELECT 
        'SUCCESS' AS Status,
        @ChargeTypesAdded AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    /*
    ================================================================================
    Error Handling: Return Detailed Error Information
    ================================================================================
    */
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        '[Eliteworks] Seed_Charge_Types.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status, ChargeTypesAdded
✅ #11 - Charge categories: Other (11) for Base Rate/Markup/Platform Charged
✅ #11 - Freight flag = 1 for 'Base Rate' only
================================================================================
*/
