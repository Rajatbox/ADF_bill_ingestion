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
         PART 2: INSERT shipment_charges (multiple rows per tracking number via CROSS APPLY)
         
         Cost Calculation: billed_shipping_cost is NOT stored in shipment_attributes.
         It's calculated on-the-fly via vw_shipment_summary view from shipment_charges
         (single source of truth). This eliminates sync issues and ensures correctness.

Sources:  billing.usps_easy_post_bill (for attributes and charges)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)
View:     billing.vw_shipment_summary (calculated billed_shipping_cost)

Charge Structure: USPS EasyPost has 5 charge types unpivoted via CROSS APPLY:
         1. Base Rate (rate column)
         2. Label Fee (label_fee column)
         3. Unknown Charges (postage_fee - rate)
         4. Carbon Offset Fee (carbon_offset_fee column)
         5. Insurance Fee (insurance_fee column)

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
    Inserts one row per unique tracking_code from usps_easypost_bill.
    
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
        u.postage_label_created_at AS shipment_date,
        u.service AS shipping_method,
        CAST(u.usps_zone AS varchar(255)) AS destination_zone,
        u.tracking_code AS tracking_number,
        
        -- Weight: Already in OZ (no conversion needed)
        u.weight AS billed_weight_oz,
        
        -- Dimensions: Already in IN (no conversion needed)
        u.[length] AS billed_length_in,
        u.width AS billed_width_in,
        u.height AS billed_height_in
    FROM
        billing.usps_easy_post_bill u
    WHERE
        u.created_at > @lastrun
        AND u.tracking_code IS NOT NULL
        AND NULLIF(TRIM(u.tracking_code), '') IS NOT NULL
        -- Idempotency: Check business key (carrier_id, tracking_number)
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_attributes sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = u.tracking_code
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: INSERT Shipment Charges
    ================================================================================
    Unpivots 5 charge types from usps_easypost_bill using CROSS APPLY.
    
    Charge Types:
    1. Base Rate - The base shipping rate (rate column)
    2. Label Fee - Fee for label generation (label_fee column)
    3. Unknown Charges - Discrepancy between postage_fee and rate (postage_fee - rate)
    4. Carbon Offset Fee - Optional carbon offset (carbon_offset_fee column)
    5. Insurance Fee - Optional insurance (insurance_fee column)
    
    Charge Category (Design Constraint #11):
    - All charges have category = 'Other' (charge_category_id = 11)
    - No 'Adjustment' charges in USPS EasyPost data
    
    Freight Flag:
    - 'Base Rate' has freight = 1 (primary shipping charge)
    - All others have freight = 0
    
    Negative Charges: Included (changed from > 0 to <> 0 per reference procedure)
    - Handles refunds and adjustments
    
    Foreign Keys:
    - shipment_attribute_id: Lookup via (carrier_id, tracking_number)
    - charge_type_id: Lookup via (carrier_id, charge_name)
    - carrier_bill_id: From usps_easypost_bill
    
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
        u.carrier_bill_id,
        u.tracking_code AS tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        billing.usps_easy_post_bill u
    -- Unpivot 5 charge types using CROSS APPLY
    CROSS APPLY (
        VALUES 
            ('Base Rate', u.rate),
            ('Label Fee', u.label_fee),
            ('Unknown Charges', u.postage_fee - u.rate),
            ('Carbon Offset Fee', u.carbon_offset_fee),
            ('Insurance Fee', u.insurance_fee)
    ) AS charges(charge_name, amount)
    -- Join to get charge_type_id
    INNER JOIN dbo.charge_types ct 
        ON ct.charge_name = charges.charge_name 
        AND ct.carrier_id = @Carrier_id
    -- Join to get shipment_attribute_id
    INNER JOIN billing.shipment_attributes sa 
        ON sa.tracking_number = u.tracking_code
        AND sa.carrier_id = @Carrier_id
    WHERE
        u.created_at > @lastrun
        AND u.carrier_bill_id IS NOT NULL
        AND u.tracking_code IS NOT NULL
        AND NULLIF(TRIM(u.tracking_code), '') IS NOT NULL
        -- Only insert non-zero charges (includes negative for refunds)
        AND charges.amount <> 0
        -- Idempotency: Check by carrier_bill_id + shipment_attribute_id + charge_type_id
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges sc
            WHERE sc.carrier_bill_id = u.carrier_bill_id
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
        '[USPS EasyPost] Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
✅ #9  - Narrow format: 5 charges unpivoted via CROSS APPLY
✅ #11 - Charge category = 'Other' (11) for all charges
✅ #11 - Freight flag = 1 for 'Base Rate' only
================================================================================

Notes:
- USPS EasyPost data is already in correct units (OZ for weight, IN for dimensions)
- Unknown Charges calculated as (postage_fee - rate) to capture discrepancies
- Negative charges included (<> 0) to handle refunds and adjustments
- All 5 charge types synced in Sync_Reference_Data.sql before this script runs
================================================================================
*/

