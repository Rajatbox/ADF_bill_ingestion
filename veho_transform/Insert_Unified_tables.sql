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

Purpose: Transform Veho carrier-specific data into unified analytical schema:
         1. Insert physical shipment attributes with unit conversions:
            - Weight: billable_weight_lb (LB × 16 → OZ)
            - Dimensions: already in IN (no conversion needed)
         2. Unpivot 3 charge slots (Charge 1/2/3) via CROSS APPLY and insert
            into shipment_charges, skipping rows where amount = 0 or charge_name IS NULL

Source:   billing.veho_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (unified physical data - NO cost stored)
          billing.shipment_charges (unified charge data)
Joins:    dbo.charge_types (charge_type_id lookup by carrier_id + charge_name)
          billing.carrier_bill (file_id filter + carrier_bill_id)

File-Based Filtering: Uses @File_id to process only the current file's data:
         - Filters veho_bill via carrier_bill JOIN on file_id

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint on (carrier_id, tracking_number)
             - Part 2: NOT EXISTS check on (shipment_attribute_id, carrier_bill_id, charge_type_id)
             - Safe to rerun with same @File_id
Transaction: NO TRANSACTION (each insert is independently idempotent)
Business Key: (carrier_id, tracking_number) - enforced by UNIQUE INDEX

Charge Category Mappings (seeded one-time):
  GP  (Ground Plus)           → Transportation (15), is_freight = 1
  DAS (Delivery Area Surch.)  → Delivery Area Surcharge (4), is_freight = 0
  ADC (Address Correction)    → Correction/Compliance (3), is_freight = 0
  Other                       → Other (11), is_freight = 0

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Shipment Attributes with Unit Conversions
    ================================================================================
    Transforms Veho physical shipment data into unified schema.
    Weight: billable_weight_lb (LB) × 16 → OZ
    Dimensions: stored in IN directly — no conversion needed.
    shipping_method already derived in Insert_ELT_&_CB.sql (NULL for ADC-only rows).
    ================================================================================
    */

    -- When a tracking_id appears multiple times in the same file, the records represent
    -- different charge types on the same shipment (e.g. GP + ADC), not double-billing.
    -- Physical attributes are the same either way — pick one row, prioritising GP over
    -- ADC via DESC on charge_code_1 ('GP' > 'ADC' alphabetically).
    -- Charges from all rows are still captured in Step 2 below.
    ;WITH deduped_attributes AS (
        SELECT
            vb.*,
            ROW_NUMBER() OVER (
                PARTITION BY vb.tracking_id
                ORDER BY vb.charge_code_1 DESC  -- GP before ADC; revisit if more codes emerge
            ) AS rn
        FROM
            billing.veho_bill AS vb
            JOIN billing.carrier_bill cb ON cb.carrier_bill_id = vb.carrier_bill_id
        WHERE
            cb.file_id = @File_id
    )
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
        vb.tracking_id AS tracking_number,
        vb.shipment_date AS shipment_date,
        vb.shipping_method AS shipping_method,
        CAST(vb.[zone] AS VARCHAR(255)) AS destination_zone,

        -- Weight conversion to OZ: Veho bills in LB
        vb.billable_weight_lb * 16.0 AS billed_weight_oz,

        -- Dimensions: Veho bills in IN — store as-is
        vb.dim_length_in AS billed_length_in,
        vb.dim_width_in AS billed_width_in,
        vb.dim_height_in AS billed_height_in

    FROM
        deduped_attributes AS vb
    WHERE
        vb.rn = 1
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes AS sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = vb.tracking_id
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Shipment Charges (Unpivot 3 Charge Slots)
    ================================================================================
    Unpivots 3 charge slots from veho_bill into normalized shipment_charges.
    Uses CROSS APPLY with VALUES to transform wide format into narrow format.
    Skips rows where amount = 0 OR charge_name IS NULL.

    Charge name stored in veho_bill is the human-readable name (e.g., 'Ground Plus',
    'Delivery Area Surcharge', 'Address Correction'). These map to charge_types via
    charge_name + carrier_id.
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
        vb.tracking_id AS tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.veho_bill AS vb

        -- Unpivot 3 charge slots using CROSS APPLY
        CROSS APPLY (
            VALUES
                (vb.charge_name_1, vb.charge_amount_1),
                (vb.charge_name_2, vb.charge_amount_2),
                (vb.charge_name_3, vb.charge_amount_3)
        ) AS charges(charge_name, amount)

        -- Join to get charge_type_id
        INNER JOIN dbo.charge_types AS ct
            ON ct.charge_name = charges.charge_name
            AND ct.carrier_id = @Carrier_id

        -- Join to get carrier_bill_id and apply file-based filtering
        INNER JOIN billing.carrier_bill AS cb
            ON cb.carrier_bill_id = vb.carrier_bill_id
            AND cb.carrier_id = @Carrier_id

        -- Join to get shipment_attribute_id
        INNER JOIN billing.shipment_attributes AS sa
            ON sa.tracking_number = ISNULL(vb.tracking_id, 'Service_charges')
            AND ISNULL(sa.carrier_id, @Carrier_id) = @Carrier_id
    WHERE
        cb.file_id = @File_id    -- File-based filtering
        AND charges.amount <> 0
        AND charges.charge_name IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges AS sc
            WHERE sc.shipment_attribute_id = sa.id
                AND sc.carrier_bill_id = cb.carrier_bill_id
                AND sc.charge_type_id = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success results
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
        'Veho Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    -- Return error details (no rollback needed - no transaction)
    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;

    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
