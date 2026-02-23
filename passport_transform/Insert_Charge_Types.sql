/*
================================================================================
ONE-TIME SEED Script: Charge Types for Passport
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ChargeTypesAdded: INT - Number of charge types added
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: ONE-TIME seed of Passport FIXED charge types into dbo.charge_types.
         Run once during initial carrier setup before the main pipeline.
         After first successful run this script will no-op (NOT EXISTS prevents duplicates).

Charge Types (6 fixed):
- Rate            (freight=1) — Base carrier shipping cost
- Fuel Surcharge  (freight=0) — Carrier fuel surcharge
- Tax             (freight=0) — Destination country tax (e.g., GST, VAT)
- Duty            (freight=0) — Import duty
- Insurance       (freight=0) — Shipment insurance
- Clearance Fee   (freight=0) — Customs clearance fee

Variable fee charge types (FEE 1–8 descriptions) are NOT seeded here.
They are discovered dynamically per-file by Sync_Reference_Data.sql Block 2,
following the FedEx pattern where description column content → charge_name.

Charge Category Mapping (Design Constraint #11):
- All charges → charge_category_id = 11 (Other)

Execution: Run once during carrier setup, before any billing data processing.
           Can be safely rerun (idempotent via NOT EXISTS check).
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Insert Fixed Charge Types (Passport — 6 Charge Types)
    ================================================================================
    Seeds 6 fixed charge types for Passport carrier.

    'Rate' is marked freight=1 (the actual carrier shipping cost).
    All other charges are freight=0 (ancillary charges).

    Variable fees (FEE 1–8) are handled dynamically by Sync_Reference_Data.sql
    Block 2, which reads FEE N DESCRIPTION columns from passport_bill and adds
    each distinct non-empty description as a new charge type automatically.
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
            (@Carrier_id, 'Rate',           1, 11),   -- Base shipping cost (freight)
            (@Carrier_id, 'Fuel Surcharge', 0, 11),   -- Fuel surcharge
            (@Carrier_id, 'Tax',            0, 11),   -- Destination tax (GST/VAT)
            (@Carrier_id, 'Duty',           0, 11),   -- Import duty
            (@Carrier_id, 'Insurance',      0, 11),   -- Shipment insurance
            (@Carrier_id, 'Clearance Fee',  0, 11)    -- Customs clearance
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
        'SUCCESS'           AS Status,
        @ChargeTypesAdded   AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    /*
    ================================================================================
    Error Handling: Return Detailed Error Information
    ================================================================================
    */
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Passport] Insert_Charge_Types.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

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
