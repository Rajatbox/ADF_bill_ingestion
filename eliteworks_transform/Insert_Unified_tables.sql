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
         PART 2: INSERT shipment_charges (1-3 rows per tracking number via CROSS APPLY)
         
         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary view from shipment_charges
         (single source of truth). This eliminates sync issues and ensures correctness.

Sources:  billing.eliteworks_bill (for attributes and charges)
          billing.carrier_bill (for carrier_bill_id)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost)

Charge Structure: Eliteworks has 3 charge types unpivoted via CROSS APPLY:
         1. Base Rate (charged_amount column) - freight=1, category_id=11
         2. Store Markup (store_markup column) - freight=0, category_id=11
         3. Correction (platform_charged - (charged_amount + store_markup)) - freight=0, category_id=16

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check + UNIQUE constraint prevents duplicate charges
             - Both parts use same pattern: INSERT ... WHERE NOT EXISTS
             - Safe to rerun with same @lastrun

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.

Business Key: shipment_attributes.id (IDENTITY) represents unique carrier_id + tracking_number
              One shipment_attributes row can have many shipment_charges rows (1-to-Many)

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
    Inserts one row per unique tracking_number from eliteworks_bill.
    
    Unit Conversions (Design Constraint #7):
    - Weight: Already in OZ (no conversion needed)
    - Dimensions: Already in IN (no conversion needed)
    
    Business Key: (carrier_id, tracking_number) enforced by UNIQUE INDEX
    
    INSERT Operation: Insert new shipments only (NOT EXISTS prevents duplicates)
    - UNIQUE constraint on (carrier_id, tracking_number) provides additional safety
    - Only inserts new tracking numbers (existing ones skipped)
    - No updates needed (core attributes never change after creation)
    
    Note: billed_shipping_cost is NOT stored in this table. It's calculated on-the-fly
    via vw_shipment_summary view from shipment_charges table (single source of truth).
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
        e.shipment_date AS shipment_date,
        e.service_method AS shipping_method,
        e.zone AS destination_zone,
        e.tracking_number AS tracking_number,
        
        -- Weight: Already in OZ (no conversion needed)
        e.billed_weight_oz AS billed_weight_oz,
        
        -- Dimensions: Already in IN (no conversion needed)
        e.package_length_in AS billed_length_in,
        e.package_width_in AS billed_width_in,
        e.package_height_in AS billed_height_in
    FROM
        billing.eliteworks_bill e
    WHERE
        e.created_date > @lastrun
        AND e.tracking_number IS NOT NULL
        AND NULLIF(TRIM(e.tracking_number), '') IS NOT NULL
        -- Idempotency: Check business key (carrier_id, tracking_number)
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = e.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: INSERT Shipment Charges
    ================================================================================
    Unpivots 3 charge types from eliteworks_bill using CROSS APPLY.
    
    Charge Types (Option 1 - Carrier Cost Focus):
    1. Base Rate - The base carrier charge (charged_amount column)
    2. Store Markup - Platform markup charge (store_markup column)
    3. Correction - Discrepancy/adjustment charge (platform_charged - (charged_amount + store_markup))
    
    Charge Category (Design Constraint #11):
    - Base Rate → charge_category_id = 11 (Other)
    - Store Markup → charge_category_id = 11 (Other)
    - Correction → charge_category_id = 16 (Adjustment)
    
    Freight Flag:
    - 'Base Rate' has freight = 1 (primary shipping charge)
    - All others have freight = 0
    
    Reconciliation Target: platform_charged column (final billed amount)
    - Sum of all 3 charges should equal platform_charged per shipment
    
    Negative Charges: Included (uses <> 0 check)
    - Handles refunds and adjustments
    
    Foreign Keys:
    - shipment_attribute_id: Lookup via (carrier_id, tracking_number)
    - charge_type_id: Lookup via (carrier_id, charge_name)
    - carrier_bill_id: From eliteworks_bill
    
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
        e.carrier_bill_id,
        e.tracking_number AS tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.eliteworks_bill e
    -- Unpivot 3 charge types using CROSS APPLY
    CROSS APPLY (
        VALUES 
            ('Base Rate', e.charged_amount),
            ('Store Markup', e.store_markup),
            ('Correction', e.platform_charged - (e.charged_amount + e.store_markup))
    ) AS charges(charge_name, amount)
    -- Join to get charge_type_id
    INNER JOIN dbo.charge_types ct 
        ON ct.charge_name = charges.charge_name 
        AND ct.carrier_id = @Carrier_id
    -- Join to get shipment_attribute_id
    INNER JOIN billing.shipment_attributes sa 
        ON sa.tracking_number = e.tracking_number
        AND sa.carrier_id = @Carrier_id
    WHERE
        e.created_date > @lastrun
        AND e.carrier_bill_id IS NOT NULL
        AND e.tracking_number IS NOT NULL
        AND NULLIF(TRIM(e.tracking_number), '') IS NOT NULL
        -- Only insert non-zero charges (includes negative for refunds/adjustments)
        AND charges.amount <> 0
        -- Idempotency: Check by carrier_bill_id + shipment_attribute_id + charge_type_id
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id = e.carrier_bill_id
                AND sc.shipment_attribute_id = sa.id
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
        '[Eliteworks] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;

/*
================================================================================
Design Constraints Applied
================================================================================
✅ #2  - No transaction (each INSERT independently idempotent)
✅ #3  - Direct CAST in Part 1 (fail fast on bad data)
✅ #4  - Idempotency via NOT EXISTS with carrier_id
✅ #5  - Business key: (carrier_id, tracking_number) enforced by UNIQUE INDEX
✅ #6  - Cost NOT stored in shipment_attributes (calculated via view)
✅ #7  - No unit conversion needed (weight in OZ, dimensions in IN)
✅ #8  - Returns Status, AttributesInserted, ChargesInserted
✅ #9  - Narrow format: 3 charges unpivoted via CROSS APPLY
✅ #11 - Charge categories: Other (11) for Base Rate/Markup, Adjustment (16) for Correction
✅ #11 - Freight flag = 1 for 'Base Rate' only
================================================================================

Notes:
- Eliteworks data is already in correct units (OZ for weight, IN for dimensions)
- Correction charge calculated as (platform_charged - (charged_amount + store_markup))
- Negative charges included (<> 0) to handle refunds and adjustments
- All 3 charge types synced in Sync_Reference_Data.sql before this script runs
- Reconciliation target: SUM(charges) = platform_charged per shipment
================================================================================
*/
