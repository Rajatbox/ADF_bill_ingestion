/*
================================================================================
ONE-TIME SEED Script: Charge Types for FlavorCloud
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

Purpose: ONE-TIME seed of FlavorCloud charge types into dbo.charge_types.
         This should be run once during initial setup before the main pipeline.
         After first successful run, this script will no-op (NOT EXISTS prevents duplicates).

Charge Types (4 total - named to match CSV column headers):
- Shipping Charges (USD) (freight=1) - Carrier shipping cost
- Commissions (USD) (freight=0) - FlavorCloud commission
- LandedCost (Duty + Taxes + Fees) (USD) (freight=0) - Combined duties, taxes, fees
- Insurance (USD) (freight=0) - Shipment insurance

Charge Category Mapping (Design Constraint #11):
- All charges → charge_category_id = 11 (Other)

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
    Insert Charge Types (FlavorCloud - 4 Charge Types)
    ================================================================================
    Seeds 4 charge types for FlavorCloud carrier. Names match CSV column headers.
    
    Shipping Charges (USD) is marked as freight=1 (the actual carrier shipping cost).
    All other charges are freight=0 (ancillary charges).
    LandedCost replaces individual Duties/Taxes/Fees to avoid granular split.
    
    After first run, this will no-op due to NOT EXISTS check.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        charge_category_id
    )
    SELECT charge_data.carrier_id, charge_data.charge_name, charge_data.freight, charge_data.charge_category_id
    FROM (
        VALUES 
            (@Carrier_id, 'Shipping Charges (USD)',                    1, 11),
            (@Carrier_id, 'Commissions (USD)',                         0, 11),
            (@Carrier_id, 'LandedCost (Duty + Taxes + Fees) (USD)',   0, 11),
            (@Carrier_id, 'Insurance (USD)',                           0, 11)
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
        '[FlavorCloud] Insert_Charge_Types.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #8  - Returns Status, ChargeTypesAdded
✅ #11 - Charge categories: All = Other (11). Only Adjustment (16) and Other (11)
         are defined. No invented categories.
================================================================================
*/
