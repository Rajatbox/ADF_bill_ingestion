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
         PART 2: INSERT shipment_charges (one row per tracking number)
         
         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary view from shipment_charges
         (single source of truth). This eliminates sync issues and ensures correctness.

Sources:  billing.eliteworks_bill (for attributes and charges)
          billing.carrier_bill (for carrier_bill_id)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost)

Charge Structure: Eliteworks stores 1 charge per shipment:
         - Platform Charged (platform_charged column) - freight=0, category_id=11
         
         Platform Charged is the authoritative billed amount (includes Base Rate + 
         Store Markup with corrections). Base Rate and Store Markup are preserved 
         in eliteworks_bill for audit but NOT stored in shipment_charges to avoid 
         double-counting.

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check + UNIQUE constraint prevents duplicate charges
             - Both parts use same pattern: INSERT ... WHERE NOT EXISTS
             - Safe to rerun with same @lastrun

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
                 Part 2 depends on Part 1 for shipment_attribute_id lookup.

Business Key: shipment_attributes.id (IDENTITY) represents unique carrier_id + tracking_number
              One shipment_attributes row maps to one shipment_charges row for Eliteworks.

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
    Inserts one charge per shipment using platform_charged as the amount.
    
    Platform Charged is the authoritative billed amount per shipment. It already
    includes Base Rate + Store Markup (with corrections applied). Storing only
    Platform Charged ensures SUM(shipment_charges.amount) = carrier_bill.total_amount
    without double-counting.
    
    Base Rate and Store Markup are preserved in eliteworks_bill for audit/reporting.
    
    Charge Category (Design Constraint #11):
    - Platform Charged → charge_category_id = 11 (Other)
    
    Zero/Negative Charges: Included (no amount filter)
    - Handles $0 shipments, refunds, and adjustments
    
    Foreign Keys:
    - shipment_attribute_id: Lookup via (carrier_id, tracking_number)
    - charge_type_id: Lookup via (carrier_id, charge_name = 'Platform Charged')
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
        e.platform_charged AS amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.eliteworks_bill e
    -- Join to get charge_type_id for 'Platform Charged'
    INNER JOIN dbo.charge_types ct 
        ON ct.charge_name = 'Platform Charged'
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
        -- Idempotency: Check by carrier_bill_id + tracking_number + charge_type_id
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id = e.carrier_bill_id
                AND sc.tracking_number = e.tracking_number
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
✅ #11 - Charge category: Other (11) for Platform Charged
================================================================================

Notes:
- Eliteworks data is already in correct units (OZ for weight, IN for dimensions)
- Only Platform Charged is stored in shipment_charges (the authoritative billed amount)
- Base Rate and Store Markup are preserved in eliteworks_bill for audit/reporting
- Zero and negative charges are included (handles $0 shipments, refunds, adjustments)
- SUM(shipment_charges.amount) = carrier_bill.total_amount (no double-counting)
================================================================================
*/
