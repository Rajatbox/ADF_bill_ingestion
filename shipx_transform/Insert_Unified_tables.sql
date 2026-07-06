/*
================================================================================
Insert Script: Unified Tables (Shipment Attributes & Charges)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - AttributesInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Transform Shipx-specific data into the unified analytical schema:
         1. Insert physical shipment attributes with unit conversions:
            - Weight: LBS × 16 → OZ
            - Dimensions: INCHES stored as-is; CASE handles other units for robustness
         2. Unpivot 2 charge columns (Delivery Charge, Fuel Surcharge) into
            shipment_charges via CROSS APPLY. Rows with amount = 0 are skipped.

Source:   billing.shipx_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (unified physical data - NO cost stored)
          billing.shipment_charges (unified charge data)
Joins:    dbo.charge_types (charge_type_id lookup)

Idempotency: - Part 1: NOT EXISTS on (carrier_id, tracking_number)
             - Part 2: NOT EXISTS on (shipment_attribute_id, carrier_bill_id, charge_type_id)
Transaction: NO TRANSACTION (each insert is independently idempotent)
Business Key: (carrier_id, tracking_number) enforced by UNIQUE INDEX

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
                 Part 2 depends on Part 1 for shipment_attribute_id.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    Step 1: Insert Shipment Attributes with Unit Conversions
    ================================================================================
    Weight source: weight column (LBS × 16 → OZ).
    Dimension source: length/width/height columns (INCHES → IN, no conversion needed).
    CASE on dims_uom retained for robustness if future files differ.
    ================================================================================
    */

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        tracking_number,
        shipment_date,
        shipping_method,
        destination_zone,
        billed_weight_oz,
        billed_length_in,
        billed_width_in,
        billed_height_in
    )
    SELECT
        @Carrier_id AS carrier_id,
        sb.tracking_number,
        sb.shipment_date,
        sb.service_level AS shipping_method,
        sb.destination_zone,

        CASE
            WHEN UPPER(TRIM(sb.weight_uom)) IN ('LB', 'LBS') THEN sb.weight * 16.0
            WHEN UPPER(TRIM(sb.weight_uom)) = 'KG'           THEN sb.weight * 35.274
            WHEN sb.weight_uom IS NULL                        THEN NULL
            ELSE sb.weight
        END AS billed_weight_oz,

        CASE
            WHEN UPPER(TRIM(sb.dims_uom)) IN ('IN', 'INCH', 'INCHES') THEN sb.length
            WHEN UPPER(TRIM(sb.dims_uom)) = 'CM'                      THEN sb.length / 2.54
            WHEN UPPER(TRIM(sb.dims_uom)) = 'MM'                      THEN sb.length / 25.4
            WHEN sb.dims_uom IS NULL                                   THEN NULL
            ELSE sb.length
        END AS billed_length_in,

        CASE
            WHEN UPPER(TRIM(sb.dims_uom)) IN ('IN', 'INCH', 'INCHES') THEN sb.width
            WHEN UPPER(TRIM(sb.dims_uom)) = 'CM'                      THEN sb.width / 2.54
            WHEN UPPER(TRIM(sb.dims_uom)) = 'MM'                      THEN sb.width / 25.4
            WHEN sb.dims_uom IS NULL                                   THEN NULL
            ELSE sb.width
        END AS billed_width_in,

        CASE
            WHEN UPPER(TRIM(sb.dims_uom)) IN ('IN', 'INCH', 'INCHES') THEN sb.height
            WHEN UPPER(TRIM(sb.dims_uom)) = 'CM'                      THEN sb.height / 2.54
            WHEN UPPER(TRIM(sb.dims_uom)) = 'MM'                      THEN sb.height / 25.4
            WHEN sb.dims_uom IS NULL                                   THEN NULL
            ELSE sb.height
        END AS billed_height_in

    FROM
        billing.shipx_bill AS sb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sb.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes AS sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = sb.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Shipment Charges (Unpivot 2 Charge Columns)
    ================================================================================
    Two charge types unpivoted via CROSS APPLY VALUES:
      - Delivery Charge: base freight charge (is_freight = 1 in charge_types seed)
      - Fuel Surcharge:  accessorial surcharge (is_freight = 0)
    Rows with amount = 0 are skipped.
    charge_type_id resolved via INNER JOIN on charge_name + carrier_id.
    ================================================================================
    */

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
        cb.carrier_bill_id,
        sb.tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.shipx_bill AS sb

        CROSS APPLY (
            VALUES
                ('Delivery Charge', sb.delivery_charge),
                ('Fuel Surcharge',  sb.fuel_surcharge)
        ) AS charges(charge_name, amount)

        INNER JOIN dbo.charge_types AS ct
            ON ct.charge_name = charges.charge_name
            AND ct.carrier_id = @Carrier_id

        INNER JOIN billing.carrier_bill AS cb
            ON cb.carrier_bill_id = sb.carrier_bill_id

        INNER JOIN billing.shipment_attributes AS sa
            ON sa.tracking_number = sb.tracking_number
            AND sa.carrier_id = @Carrier_id
    WHERE
        cb.file_id = @File_id
        AND charges.amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges AS sc
            WHERE sc.shipment_attribute_id = sa.id
                AND sc.carrier_bill_id = cb.carrier_bill_id
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
        'Shipx Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
