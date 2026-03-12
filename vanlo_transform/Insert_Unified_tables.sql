/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges (Vanlo)
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

Purpose: Two-part idempotent population script:
         PART 1: INSERT shipment_attributes (one row per tracking number)
         PART 2: INSERT shipment_charges (one row per tracking number — single charge)

         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary from shipment_charges
         (single source of truth).

Sources:  billing.vanlo_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup for 'Transportation Cost')
          dbo.carrier (integrated_carrier_id lookup)
          dbo.shipping_method (integrated_carrier_id propagation)

File-Based Filtering: Uses @File_id to process only the current file's data.

Charge Structure: Vanlo stores 1 charge per shipment:
         - Transportation Cost (cost column) — freight=1, charge_category_id=11

Unit Conversions: NONE — weight already in OZ, dimensions already in IN.

Idempotency: - Part 1: NOT EXISTS + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS + UNIQUE constraint prevents duplicate charges

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.

No Transaction: Each INSERT is independently idempotent via unique constraints.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    PART 1: INSERT Shipment Attributes
    ================================================================================
    Inserts one row per unique tracking_number from vanlo_bill.

    Unit Conversions: NONE — weight already in OZ, dimensions already in IN.

    Integrated Carrier ID: Looked up via shipping_method table (aggregator pattern).
    Join chain: vanlo_bill → carrier (on integrated_carrier) → shipping_method
                (on carrier_id, method_name, integrated_carrier_id)

    Business Key: (carrier_id, tracking_number) enforced by UNIQUE INDEX.
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
        billed_height_in,
        integrated_carrier_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        v.shipment_date AS shipment_date,
        v.service_method AS shipping_method,
        v.zone AS destination_zone,
        v.tracking_number AS tracking_number,
        v.weight_oz AS billed_weight_oz,
        v.package_length_in AS billed_length_in,
        v.package_width_in AS billed_width_in,
        v.package_height_in AS billed_height_in,
        sm.integrated_carrier_id
    FROM billing.vanlo_bill v
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = v.carrier_bill_id
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(v.integrated_carrier)
    LEFT JOIN dbo.shipping_method sm
        ON sm.carrier_id = @Carrier_id
        AND sm.method_name = v.service_method
        AND sm.integrated_carrier_id = c.carrier_id
    WHERE
        cb.file_id = @File_id
        AND NULLIF(TRIM(v.tracking_number), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = v.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: INSERT Shipment Charges
    ================================================================================
    Inserts one charge per shipment using cost as the amount.

    Vanlo has a single cost column per shipment. The charge type is
    'Transportation Cost' (seeded via Seed_Charge_Types.sql).

    Foreign Keys:
    - shipment_attribute_id: Lookup via (carrier_id, tracking_number)
    - charge_type_id: Lookup via (carrier_id, charge_name = 'Transportation Cost')
    - carrier_bill_id: From vanlo_bill

    Idempotency: UNIQUE constraint on (carrier_bill_id, tracking_number, charge_type_id)
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
        v.carrier_bill_id,
        v.tracking_number,
        ct.charge_type_id,
        v.cost AS amount,
        sa.id AS shipment_attribute_id
    FROM billing.vanlo_bill v
    INNER JOIN dbo.charge_types ct
        ON ct.charge_name = 'Transportation Cost'
        AND ct.carrier_id = @Carrier_id
    INNER JOIN billing.shipment_attributes sa
        ON sa.tracking_number = v.tracking_number
        AND sa.carrier_id = @Carrier_id
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = v.carrier_bill_id
    WHERE
        cb.file_id = @File_id
        AND v.carrier_bill_id IS NOT NULL
        AND v.cost IS NOT NULL
        AND v.cost <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id = v.carrier_bill_id
                AND sc.tracking_number = v.tracking_number
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
        '[Vanlo] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;
