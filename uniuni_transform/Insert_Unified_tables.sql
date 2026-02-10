/*
================================================================================
Insert Script: Unified Tables (Shipment Attributes & Charges)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - UniUni carrier ID (from LookupCarrierInfo.sql)
    - @lastrun: DATETIME2 - Last successful run timestamp for incremental processing
                            Filters created_date to process only new/updated records
                            Defaults to '2000-01-01' for first run
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - AttributesInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Transform carrier-specific data into unified analytical schema:
         1. Insert physical shipment attributes with unit conversions:
            - Weight: LBS/OZS → OZ (LBS × 16)
            - Dimensions: CM/IN → IN (CM ÷ 2.54)
         2. Unpivot 15 charge columns and insert into shipment_charges

Source:   Test.uniuni_bill (carrier-specific line items)
Targets:  Test.shipment_attributes (unified physical data)
          Test.shipment_charges (unified charge data)

Idempotency: Uses NOT EXISTS with carrier_id and business keys
Transaction: NO TRANSACTION (each insert is independently idempotent)
Business Key: (carrier_id, tracking_number) - enforced by UNIQUE INDEX

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT = 0;
DECLARE @ChargesInserted INT = 0;
DECLARE @lastrun DATETIME2 = '2000-01-01';  -- Default for first run, overridden by ADF parameter

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Shipment Attributes with Unit Conversions
    ================================================================================
    Transforms carrier-specific physical shipment data into unified schema.
    Converts all weight to OZ and all dimensions to IN per design constraints.
    
    Unit Conversions (preserving reference stored procedure logic):
    - Weight: LBS × 16 → OZ, OZS → OZ (no conversion)
    - Dimensions: CM × 0.393701 → IN, IN → IN (no conversion)
    
    Incremental Processing: Filters by created_date > @lastrun to only process
    new records since last successful run.
    ================================================================================
    */

    INSERT INTO Test.shipment_attributes (
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
        ub.tracking_number,
        ub.shipment_date AS shipment_date,
        ub.service_type AS shipping_method,
        CAST(ub.[zone] AS VARCHAR(255)) AS destination_zone,
        
        -- Weight conversion to OZ (from billable_weight)
        CASE 
            WHEN UPPER(ub.billable_weight_uom) = 'LBS' THEN ub.billable_weight * 16.0
            WHEN UPPER(ub.billable_weight_uom) = 'OZS' THEN ub.billable_weight
            WHEN ub.billable_weight_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_weight_oz,
        
        -- Dimension conversions to IN
        CASE 
            WHEN UPPER(ub.package_dim_uom) = 'CM' THEN ub.dim_length / 2.54
            WHEN UPPER(ub.package_dim_uom) = 'IN' THEN ub.dim_length
            WHEN ub.package_dim_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_length_in,
        
        CASE 
            WHEN UPPER(ub.package_dim_uom) = 'CM' THEN ub.dim_width / 2.54
            WHEN UPPER(ub.package_dim_uom) = 'IN' THEN ub.dim_width
            WHEN ub.package_dim_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_width_in,
        
        CASE 
            WHEN UPPER(ub.package_dim_uom) = 'CM' THEN ub.dim_height / 2.54
            WHEN UPPER(ub.package_dim_uom) = 'IN' THEN ub.dim_height
            WHEN ub.package_dim_uom IS NULL THEN NULL
            ELSE NULL  -- Unknown unit
        END AS billed_height_in
        
    FROM
        Test.uniuni_bill AS ub
    WHERE
        ub.created_date > @lastrun    -- Incremental filter: only new records
        AND NOT EXISTS (
            SELECT 1
            FROM Test.shipment_attributes AS sa
            WHERE sa.carrier_id = @Carrier_id
                AND sa.tracking_number = ub.tracking_number
        );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Shipment Charges (Unpivot Wide Format)
    ================================================================================
    Unpivots 18 charge columns from uniuni_bill into normalized shipment_charges.
    Uses CROSS APPLY with VALUES to transform wide format into narrow format.
    Only inserts charges with amount > 0.
    
    Charge Types (matching Sync_Reference_Data.sql):
    - Base Rate, Discount Fee, Discount Percentage, Signature Fee, Pick Up Fee
    - Over Dimension Fee, Over Max Size Fee, Over Weight Fee, Fuel Surcharge
    - Peak Season Surcharge, Delivery Area Surcharge, Delivery Area Surcharge Extend
    - Truck Fee, Relabel Fee, Miscellaneous Fee, Credit Card Surcharge
    - Credit, Approved Claim
    
    Incremental Processing: Filters by created_date > @lastrun to only process
    new records since last successful run.
    ================================================================================
    */

    INSERT INTO Test.shipment_charges (
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
        ub.tracking_number,
        ct.charge_type_id,
        charges.amount,
        sa.id AS shipment_attribute_id
    FROM
        Test.uniuni_bill AS ub
        
        -- Unpivot charge columns using CROSS APPLY
        CROSS APPLY (
            VALUES 
                ('Base Rate', ub.base_fee),
                ('Discount Fee', ub.discount_fee),
                ('Discount Percentage', ub.discount_percentage),
                ('Signature Fee', ub.signature_fee),
                ('Pick Up Fee', ub.pickup_fee),
                ('Over Dimension Fee', ub.over_dimension_fee),
                ('Over Max Size Fee', ub.over_max_size_fee),
                ('Over Weight Fee', ub.over_weight_fee),
                ('Fuel Surcharge', ub.fuel_surcharge),
                ('Peak Season Surcharge', ub.peak_season_surcharge),
                ('Delivery Area Surcharge', ub.delivery_area_surcharge),
                ('Delivery Area Surcharge Extend', ub.delivery_area_surcharge_extend),
                ('Truck Fee', ub.truck_fee),
                ('Relabel Fee', ub.relabel_fee),
                ('Miscellaneous Fee', ub.miscellaneous_fee),
                ('Credit Card Surcharge', ub.credit_card_surcharge),
                ('Credit', ub.credit),
                ('Approved Claim', ub.approved_claim)
        ) AS charges(charge_name, amount)
        
        -- Join to get charge_type_id
        INNER JOIN test.charge_types AS ct 
            ON ct.charge_name = charges.charge_name 
            AND ct.carrier_id = @Carrier_id
        
        -- Join to get carrier_bill_id
        INNER JOIN Test.carrier_bill AS cb
            ON cb.bill_number = CAST(ub.invoice_number AS VARCHAR(100))
            AND cb.bill_date = ub.invoice_time
            AND cb.carrier_id = @Carrier_id
        
        -- Join to get shipment_attribute_id
        INNER JOIN Test.shipment_attributes AS sa
            ON sa.tracking_number = ub.tracking_number
            AND sa.carrier_id = @Carrier_id
    WHERE
        ub.created_date > @lastrun    -- Incremental filter: only new records
        AND charges.amount > 0        -- Only insert non-zero charges
        AND NOT EXISTS (
            SELECT 1
            FROM Test.shipment_charges AS sc
            WHERE sc.carrier_id = @Carrier_id
                AND sc.carrier_bill_id = cb.carrier_bill_id
                AND sc.tracking_number = ub.tracking_number
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
    -- Return error details (no rollback needed - no transaction)
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;

END CATCH;
