/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
                        (Bukuship the aggregator carrier)
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
                 Source row = the row where charge_type = 'Freight Charge'
                 (carries canonical weight, service, and zone).
                 Accessorial-only tracking numbers (no Freight Charge row)
                 are captured using any available row as fallback.
         PART 2: INSERT shipment_charges (one row per charge row in bukuship_bill)
                 Narrow format — each bukuship_bill row is already one charge.

Carrier Model:
         Bukuship is the aggregator (is_aggregator = 1 in dbo.carrier).
         Each shipment row carries a carrier_name identifying the integrated
         carrier that physically fulfilled the shipment:
           - "Landmark Global"  → fulfilled directly by Landmark Global
           - "DHL eCommerce"    → fulfilled by DHL eCommerce via Bukuship

Tracking Number:
         - Landmark Global rows: tracking_number column (stored in bukuship_bill)
         - DHL eCommerce rows: '420' + ReceiverZipCode + WaybillNumber
           (already resolved and stored in bukuship_bill.tracking_number by
            Insert_ELT_&_CB.sql)

Unit Conversions (Design Constraint #7):
         - Weight (BilledWeight / BilledWeightUnits): OZ → OZ, LB → ×16, KG → ×35.274
         - Dimensions (Length/Width/Height): already NULL when 0; no unit column
           present so stored as-is (data observed in IN based on DimDivisor=139)

Integrated Carrier: resolved from bukuship_bill.carrier_name via dbo.carrier lookup,
         stored in shipment_attributes.integrated_carrier_id.

Sources:  billing.bukuship_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types, dbo.shipping_method, dbo.carrier

No Transaction: Each INSERT is independently idempotent
Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    PART 1: INSERT Shipment Attributes
    ================================================================================
    One row per distinct tracking_number from bukuship_bill.

    Canonical source row selection:
      - Prefer the row where charge_type = 'Freight Charge' (carries shipment weight,
        zone, and service). This is the primary billing row for each package.
      - If no Freight Charge row exists for that tracking number (accessorial-only),
        fall back to any row (MIN carrier_bill_id used as tiebreaker).

    Implemented via ROW_NUMBER() OVER (PARTITION BY tracking_number ORDER BY
      CASE charge_type WHEN 'Freight Charge' THEN 0 ELSE 1 END, carrier_bill_id).

    Weight conversion to OZ:
      billed_weight + billed_weight_units columns used (represents what was billed).
      package_weight is the physical scan weight; billed_weight accounts for dim billing.

    Dimensions:
      Already stored as NULL when 0 in bukuship_bill (handled in Insert_ELT_&_CB.sql).
      DimDivisor = 139 confirms inches. Stored as-is (no unit conversion needed).
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
        @Carrier_id                                AS carrier_id,
        NULLIF(TRIM(bb.ship_date), '')             AS shipment_date,
        bb.service_name                            AS shipping_method,
        NULLIF(TRIM(bb.zone), '')                  AS destination_zone,
        bb.tracking_number,

        -- Weight → OZ (Design Constraint #7)
        CASE
            WHEN UPPER(bb.billed_weight_units) = 'OZ' THEN bb.billed_weight
            WHEN UPPER(bb.billed_weight_units) = 'LB' THEN bb.billed_weight * 16
            WHEN UPPER(bb.billed_weight_units) = 'KG' THEN bb.billed_weight * 35.274
            ELSE bb.billed_weight
        END                                        AS billed_weight_oz,

        -- Dimensions already in IN (NULL when 0, set in Insert_ELT_&_CB.sql)
        bb.length                                  AS billed_length_in,
        bb.width                                   AS billed_width_in,
        bb.height                                  AS billed_height_in,

        c.carrier_id                               AS integrated_carrier_id

    FROM (
        SELECT
            bb.*,
            ROW_NUMBER() OVER (
                PARTITION BY bb.tracking_number
                ORDER BY
                    CASE WHEN LOWER(bb.charge_type) = 'freight charge' THEN 0 ELSE 1 END,
                    bb.carrier_bill_id
            ) AS rn
        FROM billing.bukuship_bill bb
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = bb.carrier_bill_id
        WHERE cb.file_id = @File_id
          AND NULLIF(TRIM(bb.tracking_number), '') IS NOT NULL
    ) bb
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(bb.carrier_name)
    WHERE bb.rn = 1
      AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id      = @Carrier_id
              AND sa.tracking_number = bb.tracking_number
      );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: INSERT Shipment Charges
    ================================================================================
    One row per bukuship_bill charge row (already narrow — each row = one charge).

    charge_type_id: looked up from dbo.charge_types by (carrier_id, charge_name)
    shipment_attribute_id: looked up from shipment_attributes by (carrier_id, tracking_number)

    Zero-amount charges excluded (NetCost = 0 skipped).

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
        @Carrier_id           AS carrier_id,
        bb.carrier_bill_id,
        bb.tracking_number,
        ct.charge_type_id,
        bb.net_cost           AS amount,
        sa.id                 AS shipment_attribute_id
    FROM billing.bukuship_bill bb
    JOIN billing.carrier_bill cb
        ON cb.carrier_bill_id = bb.carrier_bill_id
    INNER JOIN dbo.charge_types ct
        ON ct.charge_name  = bb.charge_name
        AND ct.carrier_id  = @Carrier_id
    INNER JOIN billing.shipment_attributes sa
        ON sa.tracking_number = ISNULL(bb.tracking_number, 'Service_charges')
        AND sa.carrier_id     = @Carrier_id
    WHERE cb.file_id = @File_id
      AND bb.net_cost IS NOT NULL
      AND bb.net_cost <> 0
      AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id  = bb.carrier_bill_id
              AND sc.tracking_number  = bb.tracking_number
              AND sc.charge_type_id   = ct.charge_type_id
      );

    SET @ChargesInserted = @@ROWCOUNT;

    SELECT
        'SUCCESS'            AS [Status],
        @AttributesInserted  AS AttributesInserted,
        @ChargesInserted     AS ChargesInserted;

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine    INT            = ERROR_LINE();
    DECLARE @ErrorNumber  INT            = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        '[Bukuship] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;

    SELECT
        'ERROR'        AS [Status],
        @ErrorNumber   AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine     AS ErrorLine;

    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (each INSERT independently idempotent)
✅ #3  - Direct CAST in Insert_ELT_&_CB.sql (fail fast); no TRY_CAST
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #5  - Business key: (carrier_id, tracking_number) enforced by UNIQUE INDEX
✅ #6  - Cost NOT stored in shipment_attributes (calculated via view)
✅ #7  - BilledWeight converted to OZ (OZ→OZ, LB×16, KG×35.274)
         Dimensions stored as NULL when 0; in inches (DimDivisor=139 confirms IN)
✅ #8  - Returns Status, AttributesInserted, ChargesInserted
✅ #10 - Narrow format: one bukuship_bill row = one charge, no unpivot needed
✅ #11 - charge_category_id = 11 (Other) for all charges
✅ #12 - Joins carrier_bill and filters by @File_id in both parts
Aggregator rule: Bukuship is the aggregator (@Carrier_id); integrated_carrier_id
                 resolved from carrier_name (Landmark Global / DHL eCommerce) → dbo.carrier
================================================================================
*/
