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

Purpose: Two-part idempotent population script:
         PART 1: INSERT shipment_attributes (one row per tracking number)
         PART 2: INSERT shipment_charges (up to 14 rows per tracking number via unpivot)

         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary from shipment_charges
         (single source of truth). This eliminates sync issues.

Sources:  billing.passport_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)

File-Based Filtering: Uses @File_id to process only the current file's data.

Charge Structure: Passport stores charges as wide columns per shipment.
         Fixed charges (always present — names hardcoded):
           - Rate           (freight=1) — base shipping cost
           - Fuel Surcharge (freight=0)
           - Tax            (freight=0)
           - Duty           (freight=0)
           - Insurance      (freight=0)
           - Clearance Fee  (freight=0)
         Variable charges (FEE 1–8 — names read from description columns, FedEx pattern):
           - FEE N DESCRIPTION column → charge_name (looked up in dbo.charge_types)
           - FEE N AMOUNT column     → charge_amount

         All 14 slots are unpivoted via OUTER APPLY VALUES into individual rows.
         Rows where charge_name is empty/NULL or charge_amount is zero are skipped.
         Sync_Reference_Data.sql must run first to seed FEE descriptions into
         dbo.charge_types before this script can match them.

Unit Conversions (Design Constraint #7):
         - Weight:     NONE required — [BILLABLE WEIGHT (OZ)] already in OZ
         - Dimensions: NONE required — [LENGTH (IN)], [WIDTH (IN)], [HEIGHT (IN)] already in IN

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check prevents duplicate charges
             Safe to rerun with same @File_id.

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.

Business Key: shipment_attributes.id (IDENTITY) represents unique carrier_id + tracking_number

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
    Inserts one row per unique tracking_number from passport_bill.

    Unit Conversions: NONE — Passport CSV already provides:
    - Weight in OZ ([BILLABLE WEIGHT (OZ)] used as billed weight)
    - Dimensions in IN ([LENGTH (IN)], [WIDTH (IN)], [HEIGHT (IN)])

    Business Key: (carrier_id, tracking_number) enforced by UNIQUE INDEX.
    Note: billed_shipping_cost is NOT stored here (calculated via view).
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
        @Carrier_id             AS carrier_id,
        p.ship_date             AS shipment_date,
        p.service_level         AS shipping_method,
        NULL                    AS destination_zone,
        p.tracking_number       AS tracking_number,

        -- Weight: already in OZ (Design Constraint #7 - no conversion needed)
        p.billable_weight_oz    AS billed_weight_oz,

        -- Dimensions: already in IN (Design Constraint #7 - no conversion needed)
        p.length_in             AS billed_length_in,
        p.width_in              AS billed_width_in,
        p.height_in             AS billed_height_in

    FROM billing.passport_bill p
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = p.carrier_bill_id
    WHERE
        cb.file_id = @File_id  -- FILE-BASED FILTERING
        AND p.tracking_number IS NOT NULL
        AND NULLIF(TRIM(p.tracking_number), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id      = @Carrier_id
                AND sa.tracking_number = p.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: INSERT Shipment Charges (Unpivot Wide → Narrow, FedEx Pattern)
    ================================================================================
    Unpivots fixed and variable charge columns from passport_bill into individual
    rows in shipment_charges using OUTER APPLY VALUES.

    Fixed charges use hardcoded names (seeded in Insert_Charge_Types.sql):
    - 'Rate'            → p.rate            (freight=1, base shipping cost)
    - 'Fuel Surcharge'  → p.fuel_surcharge  (freight=0)
    - 'Tax'             → p.tax             (freight=0)
    - 'Duty'            → p.duty            (freight=0)
    - 'Insurance'       → p.insurance       (freight=0)
    - 'Clearance Fee'   → p.clearance_fee   (freight=0)

    Variable charges read their name from the description column (FedEx pattern):
    - p.fee_1_description → charge_name, p.fee_1_amount → charge_amount
    - p.fee_2_description → charge_name, p.fee_2_amount → charge_amount
    - ... (up to FEE 8)

    Rows where charge_name is NULL/empty OR charge_amount is NULL/zero are skipped.
    Sync_Reference_Data.sql must run before this (Block 2 seeds fee descriptions
    into dbo.charge_types so the INNER JOIN can resolve charge_type_id).

    Foreign Keys:
    - shipment_attribute_id: Lookup via (carrier_id, tracking_number)
    - charge_type_id:        Lookup via (carrier_id, charge_name) from dbo.charge_types
    - carrier_bill_id:       From passport_bill

    Idempotency: NOT EXISTS on (carrier_bill_id, tracking_number, charge_type_id)
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
        @Carrier_id         AS carrier_id,
        p.carrier_bill_id,
        p.tracking_number,
        ct.charge_type_id,
        v.charge_amount     AS amount,
        sa.id               AS shipment_attribute_id
    FROM billing.passport_bill p
    OUTER APPLY (
        VALUES
            -- Fixed charges: names hardcoded, always seeded in dbo.charge_types
            ('Rate',                p.rate),
            ('Fuel Surcharge',      p.fuel_surcharge),
            ('Tax',                 p.tax),
            ('Duty',                p.duty),
            ('Insurance',           p.insurance),
            ('Clearance Fee',       p.clearance_fee),
            -- Variable charges: name comes from description column (FedEx pattern)
            (p.fee_1_description,   p.fee_1_amount),
            (p.fee_2_description,   p.fee_2_amount),
            (p.fee_3_description,   p.fee_3_amount),
            (p.fee_4_description,   p.fee_4_amount),
            (p.fee_5_description,   p.fee_5_amount),
            (p.fee_6_description,   p.fee_6_amount),
            (p.fee_7_description,   p.fee_7_amount),
            (p.fee_8_description,   p.fee_8_amount)
    ) v(charge_name, charge_amount)
    INNER JOIN dbo.charge_types ct
        ON ct.charge_name = v.charge_name
        AND ct.carrier_id = @Carrier_id
    INNER JOIN billing.shipment_attributes sa
        ON sa.tracking_number = p.tracking_number
        AND sa.carrier_id     = @Carrier_id
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = p.carrier_bill_id
    WHERE
        cb.file_id = @File_id  -- FILE-BASED FILTERING
        AND p.carrier_bill_id IS NOT NULL
        AND p.tracking_number IS NOT NULL
        AND NULLIF(TRIM(p.tracking_number), '') IS NOT NULL
        AND NULLIF(TRIM(v.charge_name), '') IS NOT NULL    -- skip empty fee descriptions
        AND v.charge_amount IS NOT NULL
        AND v.charge_amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id  = p.carrier_bill_id
                AND sc.tracking_number  = p.tracking_number
                AND sc.charge_type_id   = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Return Results
    ================================================================================
    */
    SELECT
        'SUCCESS'           AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted    AS ChargesInserted;

END TRY
BEGIN CATCH
    /*
    ================================================================================
    Error Handling: Return Detailed Error Information
    ================================================================================
    Note: No transaction to rollback - each INSERT is independently idempotent
    ================================================================================
    */
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Passport] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'          AS Status,
        @ErrorNumber     AS ErrorNumber,
        @DetailedError   AS ErrorMessage,
        @ErrorLine       AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (each INSERT independently idempotent)
✅ #3  - Direct CAST in passport_bill (fail fast on bad data)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #5  - Business key: (carrier_id, tracking_number) enforced by UNIQUE INDEX
✅ #6  - Cost NOT stored in shipment_attributes (calculated via vw_shipment_summary)
✅ #7  - No unit conversions needed: weights already OZ, dims already IN
✅ #8  - Returns Status, AttributesInserted, ChargesInserted
✅ #10 - Wide format: OUTER APPLY VALUES unpivots up to 14 charge slots → narrow rows
✅ #11 - Charge categories: All = Other (11), no invented categories
✅ #12 - File-based filtering: joins carrier_bill, filters by file_id

Notes:
- Passport weights are in OZ per column header → no OZ conversion applied
- Passport dimensions are in IN per column header → no IN conversion applied
- FEE 1–8 variable fees: each description column is the charge_name (FedEx pattern)
  → Sync_Reference_Data.sql (Block 2) must run first to seed these into dbo.charge_types
- Empty description or zero amount → row skipped (no phantom charge records)
================================================================================
*/
