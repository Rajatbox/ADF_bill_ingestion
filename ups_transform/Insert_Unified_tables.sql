/*
================================================================================
Insert Unified Tables Script (UPS)
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @carrier_id: INT - UPS carrier_id from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - ShipmentsInserted: INT - Number of shipment_attributes records inserted
    - ChargesInserted: INT - Number of shipment_charges records inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Populates unified gold layer tables:
         Part 1: shipment_attributes (physical shipment data with unit conversions)
         Part 2: shipment_charges (itemized charges)

Source:   billing.ups_bill + carrier_bill JOIN (file_id filtered)
Targets:  billing.shipment_attributes
          billing.shipment_charges

File-Based Filtering: Uses @File_id to process only the current file's data via carrier_bill JOIN

Transaction: NO TRANSACTION - Each INSERT is independently idempotent
Idempotency: Safe to re-run - inserts only if not exists (carrier_id + tracking_number)
Unit Conversions: Weight → OZ, Dimensions → IN (REQUIRED per Design Constraints)

Execution Order: FOURTH in pipeline (after Sync_Reference_Data.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ShipmentsInserted INT, @ChargesInserted INT;

BEGIN TRY
    /*
    ================================================================================
    Part 1: MERGE Shipment Attributes (Physical Shipment Data)
    ================================================================================
    Uses MERGE to handle both INSERT (new tracking numbers) and UPDATE (corrections).
    
    Strategy:
    - Shipping Method: ONLY from SHP+FRT rows (business classification)
    - Physical Attributes: From ANY charge_category_code (weight/dims can appear anywhere)
    - CTE with MAX(): Consolidates multiple rows per tracking_number
    - CROSS APPLY: Validates and converts units once (DRY principle)
    - Corrections: New valid data OVERWRITES old data
    
    UNIT CONVERSIONS (Design Constraint #7):
    - Weight: LB → OZ (*16), KG → OZ (*35.274)
    - Dimensions: CM → IN (/2.54), MM → IN (/25.4)
    
    Business Key: (carrier_id, tracking_number) - UNIQUE INDEX prevents duplicates
    ================================================================================
    */

    -- Consolidate attributes from multiple rows per tracking number
    WITH ConsolidatedAttributes AS (
            SELECT 
                @carrier_id AS carrier_id,
                ub.tracking_number,
                -- Shipping method: ONLY from SHP+FRT rows (business logic)
                MAX(CASE 
                    WHEN ub.charge_category_code = 'SHP' 
                        AND ub.charge_classification_code = 'FRT'
                    THEN v.shipping_method 
                END) AS shipping_method,
                -- Physical attributes: from ANY row (MAX picks first non-NULL)
                MAX(v.transaction_date) AS shipment_date,
                MAX(v.destination_zone) AS destination_zone,
                MAX(v.billed_weight_oz) AS billed_weight_oz,
                MAX(v.billed_length_in) AS billed_length_in,
                MAX(v.billed_width_in) AS billed_width_in,
                MAX(v.billed_height_in) AS billed_height_in,
                SYSDATETIME() AS created_date
            FROM billing.ups_bill AS ub
            CROSS APPLY (
                -- Convert units (data already cleaned in Insert_ELT_&_CB.sql)
                SELECT
                    ub.transaction_date,
                    ub.charge_description AS shipping_method,
                    ub.[zone] AS destination_zone,
                    -- Weight conversion (L=Pounds, K=Kilograms, O=Ounces)
                    CASE 
                        WHEN ub.billed_weight IS NOT NULL AND ub.billed_weight <> 0 THEN
                            CASE 
                                WHEN UPPER(ub.billed_weight_unit) = 'L' THEN ub.billed_weight * 16
                                WHEN UPPER(ub.billed_weight_unit) = 'K' THEN ub.billed_weight * 35.274
                                WHEN UPPER(ub.billed_weight_unit) = 'O' THEN ub.billed_weight
                                ELSE ub.billed_weight  -- Default: assume ounces
                            END
                    END AS billed_weight_oz,
                    -- Dimension conversions
                    CASE 
                        WHEN ub.dim_length IS NOT NULL AND ub.dim_length <> 0 THEN
                            CASE 
                                WHEN UPPER(ub.dim_unit) = 'CM' THEN ub.dim_length / 2.54
                                WHEN UPPER(ub.dim_unit) = 'MM' THEN ub.dim_length / 25.4
                                WHEN UPPER(ub.dim_unit) IN ('IN', 'I') THEN ub.dim_length
                                ELSE ub.dim_length
                            END
                    END AS billed_length_in,
                    CASE 
                        WHEN ub.dim_width IS NOT NULL AND ub.dim_width <> 0 THEN
                            CASE 
                                WHEN UPPER(ub.dim_unit) = 'CM' THEN ub.dim_width / 2.54
                                WHEN UPPER(ub.dim_unit) = 'MM' THEN ub.dim_width / 25.4
                                WHEN UPPER(ub.dim_unit) IN ('IN', 'I') THEN ub.dim_width
                                ELSE ub.dim_width
                            END
                    END AS billed_width_in,
                    CASE 
                        WHEN ub.dim_height IS NOT NULL AND ub.dim_height <> 0 THEN
                            CASE 
                                WHEN UPPER(ub.dim_unit) = 'CM' THEN ub.dim_height / 2.54
                                WHEN UPPER(ub.dim_unit) = 'MM' THEN ub.dim_height / 25.4
                                WHEN UPPER(ub.dim_unit) IN ('IN', 'I') THEN ub.dim_height
                                ELSE ub.dim_height
                            END
                    END AS billed_height_in
            ) v
            JOIN billing.carrier_bill cb ON cb.carrier_bill_id = ub.carrier_bill_id
            WHERE cb.file_id = @File_id  -- File-based filtering
                AND ub.tracking_number IS NOT NULL
            GROUP BY ub.tracking_number
    )
    MERGE INTO billing.shipment_attributes AS target
    USING ConsolidatedAttributes AS source
    ON target.carrier_id = source.carrier_id
       AND target.tracking_number = source.tracking_number
    WHEN NOT MATCHED THEN
        INSERT (
            carrier_id, tracking_number, shipment_date, shipping_method, 
            destination_zone, billed_weight_oz, billed_length_in, 
            billed_width_in, billed_height_in, created_date, updated_date
        )
        VALUES (
            source.carrier_id, source.tracking_number, source.shipment_date, 
            source.shipping_method, source.destination_zone, source.billed_weight_oz, 
            source.billed_length_in, source.billed_width_in, source.billed_height_in,
            source.created_date, source.created_date
        )
    WHEN MATCHED THEN
        UPDATE SET
            -- Physical attributes: UPS may issue corrections for weight/dimensions/zone
            billed_weight_oz = COALESCE(source.billed_weight_oz, target.billed_weight_oz),
            billed_length_in = COALESCE(source.billed_length_in, target.billed_length_in),
            billed_width_in = COALESCE(source.billed_width_in, target.billed_width_in),
            billed_height_in = COALESCE(source.billed_height_in, target.billed_height_in),
            destination_zone = COALESCE(source.destination_zone, target.destination_zone),
            updated_date = SYSDATETIME();
            -- Note: shipping_method and shipment_date are master attributes (set once, never change)

    SET @ShipmentsInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Part 2: Insert Shipment Charges (Itemized Charges)
    ================================================================================
    Inserts ALL charges (one row per charge) from ups_bill.
    
    Joins:
    - shipment_attributes: To get shipment_attribute_id
    - carrier_bill: To get carrier_bill_id
    - charge_types: To get charge_type_id
    
    Idempotency: NOT EXISTS on (carrier_bill_id, tracking_number, charge_type_id)
                 enforced by UNIQUE INDEX UQ_shipment_charges_bill_tracking_charge
    
    Note: Includes ALL charge types (FRT, FSC, ACC, INF) - not just freight
    ================================================================================
    */

    INSERT INTO billing.shipment_charges (
        carrier_id,
        carrier_bill_id,
        tracking_number,
        charge_type_id,
        amount,
        shipment_attribute_id,
        created_date
    )
    SELECT
        @carrier_id AS carrier_id,
        cb.carrier_bill_id,
        ub.tracking_number,
        ct.charge_type_id,
        ub.net_amount AS amount,
        sa.id AS shipment_attribute_id,
        SYSDATETIME() AS created_date
    FROM
        billing.ups_bill AS ub
    INNER JOIN billing.shipment_attributes AS sa
        ON sa.carrier_id = @carrier_id
        AND sa.tracking_number = ub.tracking_number
    INNER JOIN billing.carrier_bill AS cb
        ON cb.carrier_id = @carrier_id
        AND cb.bill_number = ub.invoice_number
        AND cb.bill_date = ub.invoice_date
    INNER JOIN dbo.charge_types AS ct
        ON ct.carrier_id = @carrier_id
        AND ct.charge_name = ub.charge_description
    WHERE
        cb.file_id = @File_id  -- File-based filtering
        AND ub.net_amount <> 0  -- Exclude zero charges
        AND NOT EXISTS (
            SELECT 1
            FROM billing.shipment_charges AS sc
            WHERE sc.carrier_bill_id = cb.carrier_bill_id
                AND sc.tracking_number = ub.tracking_number
                AND sc.charge_type_id = ct.charge_type_id
        );

    SET @ChargesInserted = @@ROWCOUNT;

    -- Return success metrics
    SELECT
        'SUCCESS' AS Status,
        @ShipmentsInserted AS ShipmentsInserted,
        @ChargesInserted AS ChargesInserted;

END TRY
BEGIN CATCH
    -- Return descriptive error details (no rollback needed - no transaction)
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    -- Build descriptive error message
    DECLARE @DetailedError NVARCHAR(4000) = 
        'UPS Insert_Unified_tables.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
        ' (Error ' + CAST(@ErrorNumber AS NVARCHAR(10)) + '): ' + @ErrorMessage;
    
    -- Return error details
    SELECT
        'ERROR' AS Status,
        @ErrorNumber AS ErrorNumber,
        @DetailedError AS ErrorMessage,
        @ErrorLine AS ErrorLine;
    
    -- Re-throw with descriptive message
    THROW 50000, @DetailedError, 1;
END CATCH;
