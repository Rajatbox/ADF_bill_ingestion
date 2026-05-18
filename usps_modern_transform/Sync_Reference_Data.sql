/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id:    INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status:               'SUCCESS' or 'ERROR'
    - ShippingMethodsAdded: INT - New shipping methods discovered
    - ChargeTypesAdded:     INT - New charge types seeded
    - ErrorNumber:          INT (if error)
    - ErrorMessage:         NVARCHAR (if error)
    - ErrorLine:            INT (if error)

Purpose: Automatically populate and maintain reference/lookup tables.

    Block 1: Auto-discover distinct shipping_method values from usps_modern_bill
             for this file and insert any not yet in dbo.shipping_method.

    Block 2: ONE-TIME SEED of 1 static charge type: "Freight charge"
             (charge_category_id = 15 Transportation, freight = 1).
             Idempotent — skipped if already present for this carrier.

Source:  billing.usps_modern_bill + carrier_bill JOIN (file_id filtered)
Targets: dbo.shipping_method, dbo.charge_types

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @ShippingMethodsAdded INT;
DECLARE @ChargeTypesAdded     INT;

BEGIN TRY

    -- ============================================================
    -- BLOCK 1: Sync shipping methods (auto-discovered from data)
    -- Discovers distinct shipping_method values loaded into
    -- usps_modern_bill for this file and inserts any not yet
    -- registered in dbo.shipping_method for this carrier.
    -- ============================================================

    INSERT INTO dbo.shipping_method (
        carrier_id,
        method_name
    )
    SELECT DISTINCT
        @Carrier_id        AS carrier_id,
        u.shipping_method  AS method_name
    FROM billing.usps_modern_bill u
    INNER JOIN billing.carrier_bill cb
        ON cb.carrier_bill_id = u.carrier_bill_id
    WHERE cb.file_id = @File_id
      AND NULLIF(u.shipping_method, '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.shipping_method sm
          WHERE sm.method_name = u.shipping_method
            AND sm.carrier_id  = @Carrier_id
      );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    -- ============================================================
    -- BLOCK 2: ONE-TIME SEED — Charge type "Freight charge"
    -- charge_category_id = 15 (Transportation), freight = 1.
    -- Skipped on subsequent runs via NOT EXISTS.
    -- ============================================================

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        charge_category_id,
        freight
    )
    SELECT
        @Carrier_id,
        'Freight charge',
        15,   -- Transportation
        1
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.charge_name = 'Freight charge'
          AND ct.carrier_id  = @Carrier_id
    );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    SELECT
        'SUCCESS'              AS Status,
        @ShippingMethodsAdded  AS ShippingMethodsAdded,
        @ChargeTypesAdded      AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage  NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine     INT            = ERROR_LINE();
    DECLARE @ErrorNumber   INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[USPS Modern] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
