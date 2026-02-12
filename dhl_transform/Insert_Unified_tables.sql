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

Purpose: Two-part idempotent population script (no MPS logic needed for DHL):
         PART 1: INSERT shipment_attributes with tracking number resolution
         PART 2: INSERT shipment_charges with CROSS APPLY unpivot of 4 charge columns

         Tracking Number Resolution (Column 20 logic):
         - recipient_country = 'US' → use domestic_tracking_number
         - recipient_country != 'US' → use international_tracking_number

Sources:  billing.dhl_bill (for attributes and charges)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)

Unit Conversions Applied:
  - Weight: LB → OZ (× 16), KG → OZ (× 35.274), OZ → OZ (× 1)

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check prevents duplicate charges

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql completes).
================================================================================
*/

SET NOCOUNT ON;

DECLARE @AttributesInserted INT, @ChargesInserted INT;

BEGIN TRY

    /*
    ================================================================================
    PART 1: INSERT Shipment Attributes
    ================================================================================
    Each dhl_bill row = one unique shipment.
    
    Tracking number resolution (Column 20 logic):
      - recipient_country = 'US' → domestic_tracking_number
      - recipient_country != 'US' → international_tracking_number
    
    Weight conversion: billed_weight → ounces (OZ)
    ================================================================================
    */

    INSERT INTO billing.shipment_attributes (
        carrier_id,
        shipment_date,
        shipping_method,
        destination_zone,
        tracking_number,
        billed_weight_oz
    )
    SELECT 
        @Carrier_id AS carrier_id,
        dhl.shipping_date,
        dhl.shipping_method,
        dhl.[zone] AS destination_zone,
        -- Column 20 logic: recipient_country determines which tracking number to use
        CASE 
            WHEN UPPER(TRIM(dhl.recipient_country)) = 'US' THEN dhl.domestic_tracking_number
            ELSE dhl.international_tracking_number
        END AS tracking_number,
        
        -- Weight conversion: billed_weight → ounces (OZ)
        CASE 
            WHEN UPPER(dhl.billed_weight_unit) IN ('LB', 'LBS') 
                THEN dhl.billed_weight * 16.0          -- pounds to ounces
            WHEN UPPER(dhl.billed_weight_unit) IN ('KG', 'KGS') 
                THEN dhl.billed_weight * 35.274        -- kilograms to ounces
            WHEN UPPER(dhl.billed_weight_unit) = 'OZ' 
                THEN dhl.billed_weight                 -- already ounces
            ELSE dhl.billed_weight                     -- default: assume oz
        END AS billed_weight_oz
    FROM billing.dhl_bill dhl
    WHERE dhl.created_date > @lastrun
      AND dhl.carrier_bill_id IS NOT NULL
      AND CASE 
              WHEN UPPER(TRIM(dhl.recipient_country)) = 'US' THEN dhl.domestic_tracking_number
              ELSE dhl.international_tracking_number
          END IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM billing.shipment_attributes sa
          WHERE sa.carrier_id = @Carrier_id
            AND sa.tracking_number = CASE 
                WHEN UPPER(TRIM(dhl.recipient_country)) = 'US' THEN dhl.domestic_tracking_number
                ELSE dhl.international_tracking_number
            END
      );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: Insert Shipment Charges with FK Reference
    ================================================================================
    DHL uses wide format with 4 fixed charge columns. CROSS APPLY VALUES unpivots
    these into individual charge rows:
      1. Transportation Cost                 (dhl.transportation_cost)
      2. Non-Qualified Dimensional Charges   (dhl.non_qualified_dimensional_charges)
      3. Fuel Surcharge Amount               (dhl.fuel_surcharge_amount)
      4. Delivery Area Surcharge Amount      (dhl.delivery_area_surcharge_amount)
    
    Each charge row links to:
    - charge_types via charge_type_id (looked up by charge_name + carrier_id)
    - shipment_attributes via shipment_attribute_id (looked up by tracking_number)
    
    Filters: Only inserts non-NULL, non-zero charges.
    Idempotency: NOT EXISTS on (shipment_attribute_id, carrier_bill_id, charge_type_id)
    ================================================================================
    */

    ;WITH charge_source AS (
        SELECT
            @Carrier_id AS carrier_id,
            dhl.carrier_bill_id,
            CASE 
                WHEN UPPER(TRIM(dhl.recipient_country)) = 'US' THEN dhl.domestic_tracking_number
                ELSE dhl.international_tracking_number
            END AS tracking_number,
            ct.charge_type_id,
            CAST(x.amount AS decimal(18,2)) AS amount,
            sa.id AS shipment_attribute_id
        FROM 
            billing.dhl_bill dhl
        CROSS APPLY (VALUES
            (N'Transportation Cost',               dhl.transportation_cost),
            (N'Non-Qualified Dimensional Charges',  dhl.non_qualified_dimensional_charges),
            (N'Fuel Surcharge Amount',              dhl.fuel_surcharge_amount),
            (N'Delivery Area Surcharge Amount',     dhl.delivery_area_surcharge_amount)
        ) AS x(charge_name, amount)
        INNER JOIN 
            dbo.charge_types ct
            ON ct.charge_name = x.charge_name
            AND ct.carrier_id = @Carrier_id
        INNER JOIN
            billing.shipment_attributes sa
            ON sa.carrier_id = @Carrier_id
            AND sa.tracking_number = CASE 
                WHEN UPPER(TRIM(dhl.recipient_country)) = 'US' THEN dhl.domestic_tracking_number
                ELSE dhl.international_tracking_number
            END
        WHERE 
            dhl.created_date > @lastrun
            AND dhl.carrier_bill_id IS NOT NULL
            AND x.amount IS NOT NULL
            AND x.amount <> 0
    )
    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    )
    SELECT
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id
    FROM charge_source
    WHERE NOT EXISTS (
        SELECT 1 
        FROM billing.shipment_charges sc
        WHERE sc.shipment_attribute_id = charge_source.shipment_attribute_id
          AND sc.carrier_bill_id = charge_source.carrier_bill_id
          AND sc.charge_type_id = charge_source.charge_type_id
    );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success with row counts for ADF monitoring
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
        'DHL Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    -- Return error details for ADF to handle
    SELECT 
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
