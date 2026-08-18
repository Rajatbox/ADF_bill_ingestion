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

Purpose: Two-part idempotent population script (no MPS logic needed for DHL):
         PART 1: INSERT shipment_attributes with tracking number resolution
         PART 2: INSERT shipment_charges via vw_DHLCharges (38 charge columns)

         Tracking Number Resolution (overlabel-preferred, then Column 20 fallback):
         - overlabel_tracking_number is non-empty → use overlabel_tracking_number
         - recipient_country = 'US' → use domestic_tracking_number
         - recipient_country != 'US' → use international_tracking_number

Sources:  billing.dhl_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes (business key: carrier_id + tracking_number)
          billing.shipment_charges (with shipment_attribute_id FK)
Joins:    dbo.charge_types (charge_type_id lookup)

File-Based Filtering: Uses @File_id to process only the current file's data via carrier_bill JOIN

Unit Conversions Applied:
  - Weight: LB → OZ (× 16), KG → OZ (× 35.274), OZ → OZ (× 1)

Idempotency: - Part 1: NOT EXISTS check + UNIQUE constraint prevents duplicate attributes
             - Part 2: NOT EXISTS check prevents duplicate charges
             - Safe to rerun with same @File_id

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
    
    Tracking number resolution (overlabel-preferred, then Column 20 fallback):
      - overlabel_tracking_number is non-empty → overlabel_tracking_number
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
        trk.resolved_tracking_number,
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
    JOIN billing.carrier_bill cb ON cb.carrier_bill_id = dhl.carrier_bill_id
    CROSS APPLY (
        VALUES (
            CASE 
                WHEN NULLIF(TRIM(dhl.overlabel_tracking_number), '') IS NOT NULL
                    THEN dhl.overlabel_tracking_number
                WHEN UPPER(TRIM(dhl.recipient_country)) = 'US'
                    THEN dhl.domestic_tracking_number
                ELSE dhl.international_tracking_number
            END
        )
    ) trk (resolved_tracking_number)
    WHERE cb.file_id = @File_id
      AND dhl.carrier_bill_id IS NOT NULL
      AND trk.resolved_tracking_number IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM billing.shipment_attributes sa
          WHERE sa.carrier_id = @Carrier_id
            AND sa.tracking_number = trk.resolved_tracking_number
      );

    SET @AttributesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    PART 2: Insert Shipment Charges with FK Reference
    ================================================================================
    Uses vw_DHLCharges to unpivot 38 charge columns into individual charge rows.
    The view already resolves tracking numbers and filters NULL/zero amounts.
    
    Each charge row links to:
    - charge_types via charge_type_id (looked up by charge_name + carrier_id)
    - shipment_attributes via shipment_attribute_id (looked up by tracking_number)
    
    Idempotency: NOT EXISTS on (shipment_attribute_id, carrier_bill_id, charge_type_id)
    ================================================================================
    */

    ;WITH charge_source AS (
        SELECT
            @Carrier_id AS carrier_id,
            v.carrier_bill_id,
            v.tracking_number,
            ct.charge_type_id,
            v.charge_amount AS amount,
            sa.id AS shipment_attribute_id
        FROM 
            billing.vw_DHLCharges v
        INNER JOIN 
            dbo.charge_types ct
            ON ct.charge_name = v.charge_type
            AND ct.carrier_id = @Carrier_id
        INNER JOIN
            billing.shipment_attributes sa
            ON ISNULL(sa.carrier_id, @Carrier_id) = @Carrier_id
            AND sa.tracking_number = ISNULL(v.tracking_number, 'Service_charges')
        WHERE 
            v.file_id = @File_id
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
