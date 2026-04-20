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
                 Source row = the "Freight Charge" row for that tracking number,
                 which carries the canonical weight, service, and zone.
                 Accessorial-only tracking numbers (no Freight Charge row)
                 are also captured using any available row as fallback.
         PART 2: INSERT shipment_charges (one row per charge row in bukuship_bill)
                 Narrow format — each bukuship_bill row is already one charge.

Tracking Number:
         - Landmark Global rows: tracking_number column (stored in bukuship_bill)
         - DHL eCommerce rows: '420' + ReceiverZipCode + WaybillNumber
           (already resolved and stored in bukuship_bill.tracking_number by
            Insert_ELT_&_CB.sql)

Unit Conversions (Design Constraint #7):
         - Weight (BilledWeight / BilledWeightUnits): OZ → OZ, LB → ×16, KG → ×35.274
         - Dimensions (Length/Width/Height): already NULL when 0; no unit column
           present so stored as-is (data observed in IN based on DimDivisor=139)

Integrated Carrier: resolved from CarrierName via dbo.carrier lookup, passed to
         shipment_attributes.integrated_carrier_id and shipping_method join.

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
      BilledWeight + BilledWeightUnits columns used (represents what was actually billed).
      PackageWeight is the physical scan weight; BilledWeight accounts for dim billing.

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
        NULLIF(TRIM(l.ship_date), '')              AS shipment_date,
        l.service_name                             AS shipping_method,
        NULLIF(TRIM(l.zone), '')                   AS destination_zone,
        l.tracking_number,

        -- Weight → OZ (Design Constraint #7)
        CASE
            WHEN UPPER(l.billed_weight_units) = 'OZ' THEN l.billed_weight
            WHEN UPPER(l.billed_weight_units) = 'LB' THEN l.billed_weight * 16
            WHEN UPPER(l.billed_weight_units) = 'KG' THEN l.billed_weight * 35.274
            ELSE l.billed_weight
        END                                        AS billed_weight_oz,

        -- Dimensions already in IN (NULL when 0, set in Insert_ELT_&_CB.sql)
        l.length                                   AS billed_length_in,
        l.width                                    AS billed_width_in,
        l.height                                   AS billed_height_in,

        c.carrier_id                               AS integrated_carrier_id

    FROM (
        SELECT
            l.*,
            ROW_NUMBER() OVER (
                PARTITION BY l.tracking_number
                ORDER BY
                    CASE WHEN LOWER(l.charge_type) = 'freight charge' THEN 0 ELSE 1 END,
                    l.carrier_bill_id
            ) AS rn
        FROM billing.bukuship_bill l
        JOIN billing.carrier_bill cb ON cb.carrier_bill_id = l.carrier_bill_id
        WHERE cb.file_id = @File_id
          AND NULLIF(TRIM(l.tracking_number), '') IS NOT NULL
    ) l
    LEFT JOIN dbo.carrier c
        ON LOWER(c.carrier_name) = LOWER(l.carrier_name)
    WHERE l.rn = 1
      AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id      = @Carrier_id
              AND sa.tracking_number = l.tracking_number
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
        l.carrier_bill_id,
        l.tracking_number,
        ct.charge_type_id,
        l.net_cost            AS amount,
        sa.id                 AS shipment_attribute_id
    FROM billing.bukuship_bill l
    JOIN billing.carrier_bill cb
        ON cb.carrier_bill_id = l.carrier_bill_id
    INNER JOIN dbo.charge_types ct
        ON ct.charge_name  = l.charge_name
        AND ct.carrier_id  = @Carrier_id
    INNER JOIN billing.shipment_attributes sa
        ON sa.tracking_number = l.tracking_number
        AND sa.carrier_id     = @Carrier_id
    WHERE cb.file_id = @File_id
      AND NULLIF(TRIM(l.tracking_number), '') IS NOT NULL
      AND l.net_cost IS NOT NULL
      AND l.net_cost <> 0
      AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id  = l.carrier_bill_id
              AND sc.tracking_number  = l.tracking_number
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
Aggregator rule: integrated_carrier_id resolved via CarrierName → dbo.carrier
================================================================================
*/
