/*
================================================================================
Insert Script: Unified Tables - Shipment Attributes & Charges
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - AttributesInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)
    - ErrorLine: INT (if error)

Purpose: Two-part idempotent population script:
         PART 1: INSERT shipment_attributes (one row per tracking number)
         PART 2: INSERT shipment_charges (up to 6 rows per tracking number via unpivot)
         
         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary view from shipment_charges
         (single source of truth). This eliminates sync issues and ensures correctness.

Sources:  billing.flavorcloud_bill (for attributes and charges)
          billing.carrier_bill (for carrier_bill_id)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost)

Charge Structure: FlavorCloud stores 6 charge columns per shipment (wide format):
         - Shipping Charges (freight=1) - Carrier shipping cost
         - Commissions (freight=0) - FlavorCloud commission
         - Duties (freight=0) - Import duties
         - Taxes (freight=0) - Taxes
         - Fees (freight=0) - Miscellaneous fees
         - Insurance (freight=0) - Shipment insurance
         
         These are unpivoted via CROSS APPLY VALUES into individual rows.
         Zero-amount charges are skipped (amount <> 0 filter).
         
         LandedCost is NOT included (it's Duties + Taxes + Fees and would double-count).
         Shipment Total Charges is preserved in flavorcloud_bill for audit.

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check + UNIQUE constraint prevents duplicate charges
             - Safe to rerun with same @lastrun

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.

Business Key: shipment_attributes.id (IDENTITY) represents unique carrier_id + tracking_number

No Transaction: Each INSERT is independently idempotent via unique constraints
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    PART 1: INSERT Shipment Attributes
    ================================================================================
    Inserts one row per unique tracking_number from flavorcloud_bill.
    
    Unit Conversions (Design Constraint #7):
    - Weight: CSV has Weight Unit column (LB expected) → Convert to OZ (LB × 16)
              Also handles KG → OZ (× 35.274) for safety
    - Dimensions: CSV has Dimension Unit column (IN expected) → No conversion needed
                  Also handles CM → IN (÷ 2.54) and MM → IN (÷ 25.4) for safety
    
    Business Key: (carrier_id, tracking_number) enforced by UNIQUE INDEX
    
    Note: billed_shipping_cost is NOT stored in this table.
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
        f.shipment_date AS shipment_date,
        f.service_level AS shipping_method,
        NULL AS destination_zone,
        f.tracking_number AS tracking_number,

        -- Weight conversion to OZ (Design Constraint #7)
        CASE 
            WHEN UPPER(f.weight_unit) = 'LB' THEN f.total_weight * 16
            WHEN UPPER(f.weight_unit) = 'KG' THEN f.total_weight * 35.274
            ELSE f.total_weight
        END AS billed_weight_oz,

        -- Dimension conversion to IN (Design Constraint #7)
        CASE 
            WHEN UPPER(f.dimension_unit) = 'CM' THEN f.length / 2.54
            WHEN UPPER(f.dimension_unit) = 'MM' THEN f.length / 25.4
            ELSE f.length
        END AS billed_length_in,

        CASE 
            WHEN UPPER(f.dimension_unit) = 'CM' THEN f.width / 2.54
            WHEN UPPER(f.dimension_unit) = 'MM' THEN f.width / 25.4
            ELSE f.width
        END AS billed_width_in,

        CASE 
            WHEN UPPER(f.dimension_unit) = 'CM' THEN f.height / 2.54
            WHEN UPPER(f.dimension_unit) = 'MM' THEN f.height / 25.4
            ELSE f.height
        END AS billed_height_in

    FROM
        billing.flavorcloud_bill f
    WHERE
        f.created_date > @lastrun
        AND f.tracking_number IS NOT NULL
        AND NULLIF(TRIM(f.tracking_number), '') IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = f.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: INSERT Shipment Charges (Unpivot Wide → Narrow)
    ================================================================================
    Unpivots 6 charge columns from flavorcloud_bill into individual rows in
    shipment_charges using CROSS APPLY VALUES.
    
    Charge Mapping:
    - 'Shipping Charges' → f.shipping_charges (freight=1)
    - 'Commissions'      → f.commissions      (freight=0)
    - 'Duties'           → f.duties           (freight=0)
    - 'Taxes'            → f.taxes            (freight=0)
    - 'Fees'             → f.fees             (freight=0)
    - 'Insurance'        → f.insurance        (freight=0)
    
    Zero/NULL amounts are excluded (no $0 charge records).
    
    Foreign Keys:
    - shipment_attribute_id: Lookup via (carrier_id, tracking_number)
    - charge_type_id: Lookup via (carrier_id, charge_name)
    - carrier_bill_id: From flavorcloud_bill
    
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
        f.carrier_bill_id,
        f.tracking_number,
        ct.charge_type_id,
        v.charge_amount AS amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.flavorcloud_bill f
    CROSS APPLY (
        VALUES
            ('Shipping Charges', f.shipping_charges),
            ('Commissions',      f.commissions),
            ('Duties',           f.duties),
            ('Taxes',            f.taxes),
            ('Fees',             f.fees),
            ('Insurance',        f.insurance)
    ) v(charge_name, charge_amount)
    INNER JOIN dbo.charge_types ct 
        ON ct.charge_name = v.charge_name
        AND ct.carrier_id = @Carrier_id
    INNER JOIN billing.shipment_attributes sa 
        ON sa.tracking_number = f.tracking_number
        AND sa.carrier_id = @Carrier_id
    WHERE
        f.created_date > @lastrun
        AND f.carrier_bill_id IS NOT NULL
        AND f.tracking_number IS NOT NULL
        AND NULLIF(TRIM(f.tracking_number), '') IS NOT NULL
        AND v.charge_amount IS NOT NULL
        AND v.charge_amount <> 0
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id = f.carrier_bill_id
                AND sc.tracking_number = f.tracking_number
                AND sc.charge_type_id = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Success: Return Results
    ================================================================================
    */
    SELECT 
        'SUCCESS' AS Status,
        @AttributesInserted AS AttributesInserted,
        @ChargesInserted AS ChargesInserted;

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
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        '[FlavorCloud] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (each INSERT independently idempotent)
✅ #3  - Direct CAST in flavorcloud_bill (fail fast on bad data)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #5  - Business key: (carrier_id, tracking_number) enforced by UNIQUE INDEX
✅ #6  - Cost NOT stored in shipment_attributes (calculated via view)
✅ #7  - Weight converted LB → OZ (× 16), dimensions handled IN/CM/MM → IN
✅ #8  - Returns Status, AttributesInserted, ChargesInserted
✅ #10 - Wide format: CROSS APPLY VALUES unpivots 6 charge columns → narrow rows
✅ #11 - Charge categories: All = Other (11), no invented categories
================================================================================

Notes:
- FlavorCloud weight is in LB → converted to OZ (× 16) per Design Constraint #7
- FlavorCloud dimensions are in IN → no conversion needed (but CM/MM handled for safety)
- Zero-amount charges are excluded (no $0.00 rows in shipment_charges)
- LandedCost excluded from charges (it's Duties + Taxes + Fees, would double-count)
- SUM(shipment_charges.amount) should equal SUM(shipment_total_charges) from flavorcloud_bill
================================================================================
*/
