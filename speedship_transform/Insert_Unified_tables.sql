/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - AttributesInserted: INT
    - ChargesInserted: INT
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Populate shipment_attributes and shipment_charges from speedship_bill.
         Charges unpivoted via CROSS APPLY (8 charge type/amount pairs).

Source:  billing.speedship_bill + billing.carrier_bill
Targets: billing.shipment_attributes, billing.shipment_charges

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        shipment_date,
        shipping_method,
        destination_zone,
        tracking_number,
        billed_weight_oz,
        billed_length_in,
        billed_width_in,
        billed_height_in,
        integrated_carrier_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        s.shipment_date,
        s.service_level AS shipping_method,
        s.destination_zone,
        s.tracking_number,
        CASE
            WHEN UPPER(s.weight_unit) = 'LB' THEN s.charged_weight * 16
            WHEN UPPER(s.weight_unit) = 'KG' THEN s.charged_weight * 35.274
            ELSE s.charged_weight
        END AS billed_weight_oz,
        NULL AS billed_length_in,
        NULL AS billed_width_in,
        NULL AS billed_height_in,
        sm.integrated_carrier_id
    FROM billing.speedship_bill s
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = s.carrier_bill_id
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(s.integrated_carrier)
    LEFT JOIN dbo.shipping_method sm
        ON sm.carrier_id = @Carrier_id
        AND sm.method_name = s.service_level
        AND sm.integrated_carrier_id = c.carrier_id
    WHERE
        cb.file_id = @File_id
        AND s.tracking_number IS NOT NULL
        AND NULLIF(TRIM(s.tracking_number), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = s.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        s.carrier_bill_id,
        s.tracking_number,
        ct.charge_type_id,
        v.charge_amount AS amount,
        sa.id AS shipment_attribute_id
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
    ) v(charge_name, charge_amount)
    INNER JOIN dbo.charge_types ct
        ON ct.charge_name = v.charge_name
        AND ct.carrier_id = @Carrier_id
    INNER JOIN billing.shipment_attributes sa
        ON sa.tracking_number = s.tracking_number
        AND sa.carrier_id = @Carrier_id
    WHERE
        cb.file_id = @File_id
        AND s.carrier_bill_id IS NOT NULL
        AND s.tracking_number IS NOT NULL
        AND NULLIF(TRIM(s.tracking_number), '') IS NOT NULL
        AND v.charge_amount IS NOT NULL
        AND v.charge_amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id = s.carrier_bill_id
                AND sc.tracking_number = s.tracking_number
                AND sc.charge_type_id = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    SELECT
        'SUCCESS' AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted AS ChargesInserted;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Speedship] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
