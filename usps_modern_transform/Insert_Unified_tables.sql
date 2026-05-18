/*
================================================================================
Insert Unified Tables Script
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id:    INT - File tracking ID from parent pipeline

  OUTPUT (Query Results):
    - Status:               'SUCCESS' or 'ERROR'
    - AttributesInserted:   INT - Rows inserted into shipment_attributes
    - ChargesInserted:      INT - Rows inserted into shipment_charges
    - ErrorNumber:          INT (if error)
    - ErrorMessage:         NVARCHAR (if error)
    - ErrorLine:            INT (if error)

Purpose: Promote normalized USPS Modern data into the carrier-agnostic
         analytical layer.

    Part 1: INSERT billing.shipment_attributes (one row per tracking number).
            Unit conversions:
              - Weight: billed_weight_lb × 16 → OZ  (stored in LB in usps_modern_bill)
              - Dimensions: billed_height/length/width_in already in IN → no conversion
            destination_zone: NULL (not available in ShipHero label export).
            Business key: (carrier_id, tracking_number) — enforced by UNIQUE INDEX.
            shipment_date mapped from label_created_date.

    Part 2: INSERT billing.shipment_charges (one "Freight charge" per tracking number).
            Amount = cost from usps_modern_bill.
            NOT EXISTS on (carrier_bill_id, tracking_number, charge_type_id)
            per the unique index on shipment_charges.

Sources:  billing.usps_modern_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes
          billing.shipment_charges

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)

Notes:
- USPS Modern is a direct carrier; integrated_carrier_id = NULL.
- destination_zone not present in source data; left NULL for Load_to_gold.sql.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT;
DECLARE @ChargesInserted     INT;

BEGIN TRY

    -- ============================================================
    -- PART 1: Insert billing.shipment_attributes
    -- One row per unique (carrier_id, tracking_number).
    -- Weight: billed_weight_lb × 16 converts LB → OZ.
    -- Dimensions already in inches — stored as-is.
    -- shipment_date = label_created_date (label creation timestamp).
    -- ============================================================

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        tracking_number,
        shipment_date,
        shipping_method,
        destination_zone,
        billed_weight_oz,
        billed_height_in,
        billed_length_in,
        billed_width_in
    )
    SELECT
        @Carrier_id                 AS carrier_id,
        u.tracking_number,
        u.label_created_date        AS shipment_date,
        u.shipping_method,
        NULL                        AS destination_zone,  -- not in source data
        u.billed_weight_lb * 16     AS billed_weight_oz,  -- LB → OZ
        u.billed_height_in,
        u.billed_length_in,
        u.billed_width_in
    FROM billing.usps_modern_bill u
    INNER JOIN billing.carrier_bill cb
        ON cb.carrier_bill_id = u.carrier_bill_id
    WHERE cb.file_id = @File_id
      AND NULLIF(u.tracking_number, '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM billing.shipment_attributes sa
          WHERE sa.tracking_number = u.tracking_number
            AND sa.carrier_id      = @Carrier_id
      );

    SET @AttributesInserted = @@ROWCOUNT;

    -- ============================================================
    -- PART 2: Insert billing.shipment_charges
    -- One "Freight charge" per tracking number per carrier_bill.
    -- Amount = cost (the billed shipping cost from ShipHero).
    -- NOT EXISTS on the unique index key:
    --   (carrier_bill_id, tracking_number, charge_type_id).
    -- ============================================================

    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    )
    SELECT
        @Carrier_id         AS carrier_id,
        u.carrier_bill_id,
        u.tracking_number,
        ct.charge_type_id,
        u.cost              AS amount,
        sa.id               AS shipment_attribute_id
    FROM billing.usps_modern_bill u
    INNER JOIN billing.carrier_bill cb
        ON  cb.carrier_bill_id = u.carrier_bill_id
    INNER JOIN billing.shipment_attributes sa
        ON  sa.tracking_number = u.tracking_number
        AND sa.carrier_id      = @Carrier_id
    INNER JOIN dbo.charge_types ct
        ON  ct.charge_name  = 'Freight charge'
        AND ct.carrier_id   = @Carrier_id
    WHERE cb.file_id = @File_id
      AND NULLIF(u.tracking_number, '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM billing.shipment_charges sc
          WHERE sc.carrier_bill_id = u.carrier_bill_id
            AND sc.tracking_number = u.tracking_number
            AND sc.charge_type_id  = ct.charge_type_id
      );

    SET @ChargesInserted = @@ROWCOUNT;

    SELECT
        'SUCCESS'           AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted    AS ChargesInserted;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage  NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine     INT            = ERROR_LINE();
    DECLARE @ErrorNumber   INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[USPS Modern] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
