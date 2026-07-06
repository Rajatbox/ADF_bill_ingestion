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

Purpose: Transform Shippo carrier-specific data into unified analytical schema:
         1. Insert physical shipment attributes:
            - Weight: parcel_weight converted using parcel_mass_unit (LB→OZ, KG→OZ)
                      NULL when unit unavailable (current Shippo export omits units)
            - Dimensions: parcel_length/width/height converted using parcel_distance_unit
                          NULL when unit unavailable
            - destination_zone: NULL (not present in Shippo export)
            - integrated_carrier_id: resolved from rate_provider via shipping_method
         2. Insert shipment charges (CROSS APPLY with one slot: 'Base Rate' from rate_amount)

Source:   billing.shippo_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (unified physical data - NO cost stored)
          billing.shipment_charges (unified charge data)
Joins:    dbo.charge_types (charge_type_id lookup by carrier_id + charge_name)
          dbo.carrier (integrated carrier lookup by rate_provider)
          dbo.shipping_method (integrated_carrier_id lookup)
          billing.carrier_bill (file_id filter + carrier_bill_id)

File-Based Filtering: Uses @File_id to process only the current file's data.
Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint on (carrier_id, tracking_number)
             - Part 2: NOT EXISTS check on (shipment_attribute_id, carrier_bill_id, charge_type_id)
             - Safe to rerun with same @File_id
Transaction: NO TRANSACTION (each insert is independently idempotent)
Business Key: (carrier_id, tracking_number) - enforced by UNIQUE INDEX

Charge Type:
  'Base Rate' → Transportation (15), freight=1 (seeded in Sync_Reference_Data.sql)

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Shipment Attributes
    ================================================================================
    One row per unique tracking_number from shippo_bill.

    Weight conversion: driven by parcel_mass_unit (LB→×16, KG→×35.274, else as-is).
    Dimension conversion: driven by parcel_distance_unit (CM→÷2.54, MM→÷25.4, else as-is).
    Both fields are NULL in current Shippo export — values will be NULL in unified layer.
    integrated_carrier_id is resolved from rate_provider via shipping_method FK.
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
        billed_height_in,
        integrated_carrier_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        sb.tracking_number,
        CAST(sb.object_created AS DATETIME) AS shipment_date,
        sb.rate_servicelevel_name AS shipping_method,
        NULL AS destination_zone,  -- Not present in Shippo export

        -- Weight: convert to OZ using parcel_mass_unit when available
        CASE UPPER(sb.parcel_mass_unit)
            WHEN 'LB' THEN sb.parcel_weight * 16.0
            WHEN 'KG' THEN sb.parcel_weight * 35.274
            ELSE sb.parcel_weight   -- Already OZ, or NULL
        END AS billed_weight_oz,

        -- Dimensions: convert to IN using parcel_distance_unit when available
        CASE UPPER(sb.parcel_distance_unit)
            WHEN 'CM' THEN sb.parcel_length / 2.54
            WHEN 'MM' THEN sb.parcel_length / 25.4
            ELSE sb.parcel_length   -- Already IN, or NULL
        END AS billed_length_in,
        CASE UPPER(sb.parcel_distance_unit)
            WHEN 'CM' THEN sb.parcel_width / 2.54
            WHEN 'MM' THEN sb.parcel_width / 25.4
            ELSE sb.parcel_width
        END AS billed_width_in,
        CASE UPPER(sb.parcel_distance_unit)
            WHEN 'CM' THEN sb.parcel_height / 2.54
            WHEN 'MM' THEN sb.parcel_height / 25.4
            ELSE sb.parcel_height
        END AS billed_height_in,

        sm.integrated_carrier_id
    FROM
        billing.shippo_bill AS sb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = sb.carrier_bill_id
        LEFT JOIN dbo.carrier c ON LOWER(c.carrier_name) = LOWER(TRIM(sb.rate_provider))
        LEFT JOIN dbo.shipping_method sm
            ON sm.carrier_id = @Carrier_id
            AND sm.method_name = sb.rate_servicelevel_name
            AND sm.integrated_carrier_id = c.carrier_id
    WHERE
        cb.file_id = @File_id
        AND sb.tracking_number IS NOT NULL
        AND NULLIF(TRIM(sb.tracking_number), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes AS sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = sb.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Shipment Charges
    ================================================================================
    One charge per shipment via CROSS APPLY with a single slot.
    Shippo bills a single 'Base Rate' per label (rate_amount).
    charge_type_id resolved via charge_name + carrier_id lookup in dbo.charge_types.
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
        sb.carrier_bill_id,
        sb.tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.shippo_bill AS sb

        -- Single charge slot
        CROSS APPLY (
            VALUES ('Base Rate', sb.rate_amount)
        ) AS charges(charge_name, amount)

        -- Lookup charge_type_id
        INNER JOIN dbo.charge_types AS ct
            ON ct.charge_name = charges.charge_name
            AND ct.carrier_id = @Carrier_id

        -- File-based filtering and carrier_bill_id
        INNER JOIN billing.carrier_bill AS cb
            ON cb.carrier_bill_id = sb.carrier_bill_id
            AND cb.carrier_id = @Carrier_id

        -- Lookup shipment_attribute_id (inserted in Step 1)
        INNER JOIN billing.shipment_attributes AS sa
            ON sa.tracking_number = sb.tracking_number
            AND sa.carrier_id = @Carrier_id
    WHERE
        cb.file_id = @File_id
        AND sb.tracking_number IS NOT NULL
        AND NULLIF(TRIM(sb.tracking_number), '') IS NOT NULL
        AND charges.amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges AS sc
            WHERE sc.shipment_attribute_id = sa.id
                AND sc.carrier_bill_id = sb.carrier_bill_id
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
        'Shippo Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
