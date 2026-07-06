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
    - CarriersAdded: INT - Number of new integrated carriers auto-discovered
    - ShippingMethodsAdded: INT - Number of new shipping methods discovered
    - ChargeTypesAdded: INT - Number of new charge types seeded
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Idempotent synchronization of reference data tables:

         Block 0: Auto-discovers integrated carriers from rate_provider into dbo.carrier.
                  Ensures FK resolution before Block 1 runs.

         Block 1: Discovers distinct shipping methods from rate_servicelevel_name +
                  integrated_carrier_id from rate_provider lookup. Same pattern as EasyPost.

         Block 2: Seeds one static charge type — 'Base Rate' (Transportation/15, freight=1).
                  Shippo has a single charge per label (rate_amount). After first run this no-ops.

Source:   billing.shippo_bill + carrier_bill JOIN (file_id filtered)
Targets:  dbo.carrier (integrated carriers)
          dbo.shipping_method (service levels per carrier)
          dbo.charge_types (one-time seed)

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql).
Idempotent: Safe to run multiple times — NOT EXISTS prevents duplicates.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @CarriersAdded INT, @ShippingMethodsAdded INT, @ChargeTypesAdded INT;

BEGIN TRY

    /*
    ================================================================================
    Block 0: Auto-Discover Integrated Carriers
    ================================================================================
    Inserts any rate_provider values (e.g. USPS, FedEx) not yet in dbo.carrier.
    Must run before Block 1 so the LEFT JOIN to dbo.carrier resolves correctly.
    Inserted as is_aggregator = 0 (these are real fulfillment carriers).
    ================================================================================
    */

    INSERT INTO dbo.carrier (carrier_name, is_active, is_aggregator)
    SELECT DISTINCT
        TRIM(sb.rate_provider) AS carrier_name,
        1 AS is_active,
        0 AS is_aggregator
    FROM
        billing.shippo_bill AS sb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sb.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND sb.rate_provider IS NOT NULL
        AND NULLIF(TRIM(sb.rate_provider), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dbo.carrier c
            WHERE LOWER(c.carrier_name) = LOWER(TRIM(sb.rate_provider))
        );

    SET @CarriersAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 1: Synchronize Shipping Methods
    ================================================================================
    Discovers distinct service levels from rate_servicelevel_name and inserts any
    new methods. integrated_carrier_id is resolved from rate_provider via dbo.carrier
    (populated by Block 0). NOT EXISTS checks method_name + carrier_id + integrated_carrier_id.
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
        sb.rate_servicelevel_name AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active,
        c.carrier_id AS integrated_carrier_id
    FROM
        billing.shippo_bill AS sb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sb.carrier_bill_id
        LEFT JOIN dbo.carrier c ON LOWER(c.carrier_name) = LOWER(TRIM(sb.rate_provider))
    WHERE
        cb.file_id = @File_id
        AND sb.rate_servicelevel_name IS NOT NULL
        AND NULLIF(TRIM(sb.rate_servicelevel_name), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method sm
            WHERE sm.method_name = sb.rate_servicelevel_name
                AND sm.carrier_id = @Carrier_id
                AND sm.integrated_carrier_id = c.carrier_id
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    /*
    ================================================================================
    Block 2: ONE-TIME SEED - Charge Types
    ================================================================================
    Shippo has a single charge per label: the label rate stored in rate_amount.
    Seeded as 'Base Rate' → Transportation (15), freight=1.
    After the first run this no-ops due to NOT EXISTS.
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
        VALUES (@Carrier_id, 'Base Rate', 1, 15)  -- Transportation
    ) AS charge_data(carrier_id, charge_name, freight, charge_category_id)
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.charge_name = charge_data.charge_name
            AND ct.carrier_id = @Carrier_id
    );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    SELECT
        'SUCCESS'             AS Status,
        @CarriersAdded        AS CarriersAdded,
        @ShippingMethodsAdded AS ShippingMethodsAdded,
        @ChargeTypesAdded     AS ChargeTypesAdded;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'Shippo Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'        AS Status,
        @ErrorNumber   AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine     AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
