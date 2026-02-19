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

Purpose: ONE-TIME seed of Eliteworks charge type into dbo.charge_types.
         This should be run once during initial setup before the main pipeline.
         After first successful run, this script will no-op (NOT EXISTS prevents duplicates).

Charge Type:
- Platform Charged (freight=0) - Final billed amount ([Platform Charged (With Corrections)] column)
  This is the authoritative total per shipment. It already includes Base Rate + Store Markup.
  Base Rate and Store Markup are preserved in eliteworks_bill for audit/reporting
  but are NOT stored as separate charges in shipment_charges to avoid double-counting.

Charge Category Mapping (Design Constraint #11):
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
    Insert Charge Type (Eliteworks Single Charge)
    ================================================================================
    Seeds 1 charge type for Eliteworks carrier: Platform Charged.
    
    Platform Charged is the authoritative billed amount per shipment.
    Base Rate and Store Markup are component breakdowns preserved in 
    eliteworks_bill for audit, but NOT stored in shipment_charges.
    
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
✅ #11 - Charge category: Other (11) for Platform Charged

Note: Only Platform Charged is seeded. Base Rate and Store Markup are preserved
      in eliteworks_bill for audit but excluded from shipment_charges to prevent
      double-counting (Platform Charged = Base Rate + Store Markup).
================================================================================
*/
