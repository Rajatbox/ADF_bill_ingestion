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
    - AttributesInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-part idempotent population script. GOFO invoices per shipment
         only -- every row carries a real tracking number, so there is no
         account-level/null-tracking charge path and no Service_charges
         sentinel join needed here (see CLAUDE.md for that pattern on
         UPS/DHL/FedEx/Bukuship/UniUni/Veho):
         PART 1: INSERT shipment_attributes
         PART 2: INSERT shipment_charges, unpivoting the 13 fixed charge columns
                 inline (no view -- the mapping is static, unlike FedEx's dynamic
                 charges), including the negative 'Credit for Order Value' rows

Sources:  billing.gofo_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)

File-Based Filtering: Uses @File_id to process only the current file's data via carrier_bill JOIN

Unit Conversions Applied:
  - Weight: Invoicing weight (lbs) -> OZ (x 16)
  - Dimensions: Already in inches (no conversion) -> billed_length_in/width_in/height_in

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check prevents duplicate charges
             - Safe to rerun with same @File_id

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    PART 1: INSERT Shipment Attributes
    ================================================================================
    Each gofo_bill row = one unique shipment (including the ~30 credit-only
    adjustment rows per file, which still carry a real tracking number).

    Weight conversion: invoicing_weight_lbs -> ounces (OZ)
    Dimensions already in inches -- no conversion applied.
    ================================================================================
    */

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        shipment_date,
        shipping_method,
        destination_zone,
        tracking_number,
        billed_weight_oz,
        billed_length_in,
        billed_width_in,
        billed_height_in
    )
    SELECT
        @Carrier_id AS carrier_id,
        gofo.ascan_date,
        gofo.product,
        gofo.[zone] AS destination_zone,
        gofo.tracking_number,
        gofo.invoicing_weight_lbs * 16.0 AS billed_weight_oz,
        gofo.dim_length_in,
        gofo.dim_width_in,
        gofo.dim_height_in
    FROM billing.gofo_bill gofo
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = gofo.carrier_bill_id
    WHERE cb.file_id = @File_id
      AND gofo.tracking_number IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM billing.shipment_attributes sa
          WHERE sa.carrier_id = @Carrier_id
            AND sa.tracking_number = gofo.tracking_number
      );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: Insert Shipment Charges with FK Reference
    ================================================================================
    Unpivots the 13 fixed charge columns into individual charge rows inline
    (OUTER APPLY VALUES) -- no view needed, since the column-to-charge-name
    mapping is fixed and known at design time (unlike FedEx's genuinely
    dynamic/pivoted charges, where vw_FedExCharges earns its keep).
    Non-zero amounts only (negative 'Credit for Order Value' rows pass through).

    Each charge row links to:
    - charge_types via charge_type_id (looked up by charge_name + carrier_id)
    - shipment_attributes via shipment_attribute_id (looked up by tracking_number)

    Idempotency: NOT EXISTS on (shipment_attribute_id, carrier_bill_id, charge_type_id)
    ================================================================================
    */

    ;WITH gofo_charges AS (
        SELECT
            gofo.carrier_bill_id,
            gofo.tracking_number,
            cb.file_id,
            chg.charge_type,
            chg.charge_amount
        FROM billing.gofo_bill gofo
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = gofo.carrier_bill_id
        OUTER APPLY (
            VALUES
                (N'Delivery Fee',                   gofo.delivery_fee),
                (N'Overweight Fees',                gofo.overweight_fees),
                (N'Oversized Fees',                 gofo.oversized_fees),
                (N'Return Fees',                     gofo.return_fees),
                (N'Remote Area Fees',                gofo.remote_area_fees),
                (N'Fuel Surcharges',                 gofo.fuel_surcharges),
                (N'Delivery Area Fees',              gofo.delivery_area_fees),
                (N'Additional Interception Fees',    gofo.additional_interception_fees),
                (N'Relabelling Fee',                 gofo.relabelling_fee),
                (N'Return Reship Fee',               gofo.return_reship_fee),
                (N'Credit for Delivery Fees',        gofo.credit_for_delivery_fees),
                (N'Credit for Order Value',          gofo.credit_for_order_value),
                (N'Other',                           gofo.other_fee)
        ) chg (charge_type, charge_amount)
        WHERE chg.charge_amount IS NOT NULL
          AND chg.charge_amount <> 0
    ),
    charge_source AS (
        SELECT
            @Carrier_id AS carrier_id,
            gc.carrier_bill_id,
            gc.tracking_number,
            ct.charge_type_id,
            gc.charge_amount AS amount,
            sa.id AS shipment_attribute_id
        FROM
            gofo_charges gc
        INNER JOIN
            dbo.charge_types ct
            ON ct.charge_name = gc.charge_type
            AND ct.carrier_id = @Carrier_id
        INNER JOIN
            billing.shipment_attributes sa
            ON sa.carrier_id = @Carrier_id
            AND sa.tracking_number = gc.tracking_number
        WHERE
            gc.file_id = @File_id
    )
    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    )
    SELECT
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    FROM charge_source
    WHERE NOT EXISTS (
        SELECT 1
        FROM billing.shipment_charges sc
        WHERE sc.shipment_attribute_id = charge_source.shipment_attribute_id
          AND sc.carrier_bill_id = charge_source.carrier_bill_id
          AND sc.charge_type_id = charge_source.charge_type_id
    );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success with row counts for ADF monitoring
    SELECT
        'SUCCESS' AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted AS ChargesInserted;

END TRY
BEGIN CATCH
    -- Build descriptive error message
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'GOFO Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    -- Return error details for ADF to handle
    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
