/*
================================================================================
Reference Data Synchronization Script (Vanlo)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - CarriersAdded: INT - Number of new integrated carriers auto-discovered
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ChargeTypesAdded: INT - Number of new charge types seeded (0 or 1)
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Automatically populate and maintain reference/lookup tables by
         discovering new values from processed Vanlo billing data.

         Block 0: Auto-discover integrated carriers from billing data into dbo.carrier
         Block 1: Sync shipping methods from service_method column in vanlo_bill
                  with integrated_carrier_id FK (aggregator pattern)
         Block 2: Ensure single charge type 'Transportation Cost' exists
                  (idempotent — only inserts on first run)

Source:  billing.vanlo_bill + carrier_bill JOIN (file_id filtered)
Targets: dbo.carrier (integrated carriers)
         dbo.shipping_method (with integrated_carrier_id)
         dbo.charge_types (single charge type: Transportation Cost)

File-Based Filtering: Uses @File_id to process only the current file's data.

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql completes).
Idempotent: Safe to run multiple times - uses NOT EXISTS to prevent duplicates.
No Transaction: Each INSERT is independently idempotent via unique constraints.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @CarriersAdded INT, @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Block 0: Auto-Discover Integrated Carriers
    ================================================================================
    Vanlo routes shipments through multiple underlying carriers (USPS, etc.).
    The integrated_carrier column (extracted from Service column split) may
    reference carrier names not yet in dbo.carrier. Insert them before the
    shipping method sync so the LEFT JOIN to dbo.carrier resolves correctly.

    Inserted with is_aggregator = 0 (these are real fulfillment carriers).
    Case-insensitive NOT EXISTS to avoid duplicates like "usps" vs "USPS".
    ================================================================================
    */

    INSERT INTO dbo.carrier (carrier_name, is_active, is_aggregator)
    SELECT DISTINCT
        v.integrated_carrier AS carrier_name,
        1 AS is_active,
        0 AS is_aggregator
    FROM billing.vanlo_bill v
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = v.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND NULLIF(TRIM(v.integrated_carrier), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dbo.carrier c
            WHERE LOWER(c.carrier_name) = LOWER(v.integrated_carrier)
        );

    SET @CarriersAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 1: Synchronize Shipping Methods
    ================================================================================
    Discovers distinct service methods from vanlo_bill and inserts any new methods
    into the shipping_method table with integrated_carrier_id.

    Populates with sensible defaults:
    - carrier_id: From @Carrier_id parameter (Vanlo)
    - method_name: The service method (e.g., "GroundAdvantage", "Priority")
    - service_level: Default 'Standard'
    - guaranteed_delivery: Default 0 (false)
    - is_active: Default 1 (true)
    - integrated_carrier_id: FK to actual carrier from dbo.carrier lookup

    File-Based Filtering: Joins carrier_bill and filters by file_id.
    ================================================================================
    */

    INSERT INTO dbo.shipping_method (
        carrier_id,
        method_name,
        service_level,
        guaranteed_delivery,
        is_active,
        integrated_carrier_id
    )
    SELECT DISTINCT
        @Carrier_id AS carrier_id,
        v.service_method AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active,
        c.carrier_id AS integrated_carrier_id
    FROM billing.vanlo_bill v
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = v.carrier_bill_id
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(v.integrated_carrier)
    WHERE
        cb.file_id = @File_id
        AND NULLIF(TRIM(v.service_method), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method sm
            WHERE sm.method_name = v.service_method
                AND sm.carrier_id = @Carrier_id
                AND sm.integrated_carrier_id = c.carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 2: Ensure Charge Type Exists
    ================================================================================
    Vanlo has a single cost column per shipment mapped to 'Transportation Cost'.
    This block ensures the charge type exists for Insert_Unified_tables.sql to
    resolve charge_type_id via INNER JOIN. Only inserts on first pipeline run.
    ================================================================================
    */

    INSERT INTO dbo.charge_types (carrier_id, charge_name, freight, charge_category_id)
    SELECT @Carrier_id, 'Transportation Cost', 1, 11
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.carrier_id = @Carrier_id
            AND ct.charge_name = 'Transportation Cost'
    );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    SELECT
        'SUCCESS' AS Status,
        @CarriersAdded AS CarriersAdded,
        @ShippingMethodsAdded AS ShippingMethodsAdded,
        @ChargeTypesAdded AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Vanlo] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
