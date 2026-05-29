/*
================================================================================
Reference Data Synchronization Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Speedship aggregator carrier identifier
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - CarriersAdded: INT
    - ShippingMethodsAdded: INT
    - ChargeTypesAdded: INT
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Populate reference tables from speedship_bill for the current file:
         Block 0: Auto-discover integrated carriers (SCAC-normalized)
         Block 1: Sync shipping methods (service_level + integrated_carrier_id)
         Block 2: Seed Unknown Charge + discover charge types from 8 wide columns

Source:  billing.speedship_bill + billing.carrier_bill
Targets: dbo.carrier, dbo.shipping_method, dbo.charge_types

Execution Order: THIRD in pipeline (after Insert_ELT_&_CB.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @CarriersAdded INT = 0;
DECLARE @ShippingMethodsAdded INT = 0;
DECLARE @ChargeTypesAdded INT = 0;

BEGIN TRY

    INSERT INTO dbo.carrier (carrier_name, is_active, is_aggregator)
    SELECT DISTINCT
        s.integrated_carrier AS carrier_name,
        1 AS is_active,
        0 AS is_aggregator
    FROM billing.speedship_bill s
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = s.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND s.integrated_carrier IS NOT NULL
        AND NULLIF(TRIM(s.integrated_carrier), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM dbo.carrier c
            WHERE LOWER(c.carrier_name) = LOWER(s.integrated_carrier)
        );

    SET @CarriersAdded = @@ROWCOUNT;

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
        s.service_level AS method_name,
        'Standard' AS service_level,
        0 AS guaranteed_delivery,
        1 AS is_active,
        c.carrier_id AS integrated_carrier_id
    FROM billing.speedship_bill s
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = s.carrier_bill_id
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(s.integrated_carrier)
    WHERE
        cb.file_id = @File_id
        AND s.service_level IS NOT NULL
        AND NULLIF(TRIM(s.service_level), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.shipping_method sm
            WHERE sm.method_name = s.service_level
                AND sm.carrier_id = @Carrier_id
                AND (
                    (sm.integrated_carrier_id IS NULL AND c.carrier_id IS NULL)
                    OR sm.integrated_carrier_id = c.carrier_id
                )
        );

    SET @ShippingMethodsAdded = @@ROWCOUNT;

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        charge_category_id
    )
    SELECT
        @Carrier_id,
        'Unknown Charge',
        0,
        11
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.charge_types ct
        WHERE ct.carrier_id = @Carrier_id
            AND ct.charge_name = 'Unknown Charge'
    );

    SET @ChargeTypesAdded = @@ROWCOUNT;

    INSERT INTO dbo.charge_types (
        carrier_id,
        charge_name,
        freight,
        charge_category_id
    )
    SELECT DISTINCT
        @Carrier_id AS carrier_id,
        charges.charge_name,
        CASE WHEN charges.charge_name = 'SMALL PACKAGE FREIGHT' THEN 1 ELSE 0 END AS freight,
        CASE
            WHEN LOWER(charges.charge_name) LIKE '%adjustment%' THEN 16
            ELSE 11
        END AS charge_category_id
    FROM billing.speedship_bill s
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = s.carrier_bill_id
    CROSS APPLY (
        VALUES
            (COALESCE(NULLIF(TRIM(s.charge_type_1), ''), 'Unknown Charge'), s.charge_amount_1),
            (COALESCE(NULLIF(TRIM(s.charge_type_2), ''), 'Unknown Charge'), s.charge_amount_2),
            (COALESCE(NULLIF(TRIM(s.charge_type_3), ''), 'Unknown Charge'), s.charge_amount_3),
            (COALESCE(NULLIF(TRIM(s.charge_type_4), ''), 'Unknown Charge'), s.charge_amount_4),
            (COALESCE(NULLIF(TRIM(s.charge_type_5), ''), 'Unknown Charge'), s.charge_amount_5),
            (COALESCE(NULLIF(TRIM(s.charge_type_6), ''), 'Unknown Charge'), s.charge_amount_6),
            (COALESCE(NULLIF(TRIM(s.charge_type_7), ''), 'Unknown Charge'), s.charge_amount_7),
            (COALESCE(NULLIF(TRIM(s.charge_type_8), ''), 'Unknown Charge'), s.charge_amount_8)
    ) charges(charge_name, charge_amount)
    WHERE
        cb.file_id = @File_id
        AND charges.charge_amount IS NOT NULL
        AND charges.charge_amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM dbo.charge_types ct
            WHERE ct.carrier_id = @Carrier_id
                AND ct.charge_name = charges.charge_name
        );

    SET @ChargeTypesAdded = @ChargeTypesAdded + @@ROWCOUNT;

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
        '[Speedship] Sync_Reference_Data.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
