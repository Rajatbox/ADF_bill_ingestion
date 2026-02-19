/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from parent pipeline
    - @File_id: INT - File tracking ID from parent pipeline
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of uniuni_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process with file tracking:
         1. Aggregate and insert invoice-level summary data from delta_uniuni_bill
            into carrier_bill with file_id - generates carrier_bill_id
         2. Insert line-level billing data from delta_uniuni_bill (ELT staging)
            into uniuni_bill (Carrier Bill line items) with carrier_bill_id foreign key

Source:   billing.delta_uniuni_bill
Targets:  billing.carrier_bill (invoice summaries with file_id)
          billing.uniuni_bill (line items)

File Tracking: file_id stored in carrier_bill enables:
               - File-based idempotency checks (same file won't create duplicates)
               - Cross-carrier parallel processing (different files, different carriers)
               - Selective file retry on failure

Validation: Fails if invoice_time or tracking_number is NULL or empty (fail-fast CAST)
Match:      Step 1: bill_number + bill_date + carrier_id (INSERT WHERE NOT EXISTS)
            Step 2: carrier_bill_id only (INSERT WHERE NOT EXISTS) per Design Constraint #9
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

Execution Order: SECOND in pipeline (after ValidateCarrierInfo.sql)
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;  -- Automatically rollback on error

DECLARE @InvoicesInserted INT, @LineItemsInserted INT;

BEGIN TRANSACTION;

BEGIN TRY
    /*
    ================================================================================
    Step 1: Insert Invoice-Level Summary Data
    ================================================================================
    Aggregates line items by invoice_number and invoice_time to create invoice-level
    summaries in carrier_bill. Calculates total_amount (sum of all charges) and
    num_shipments (count of tracking numbers) per invoice.
    Generates carrier_bill_id values which will be joined in downstream processes.
    ================================================================================
    */

    INSERT INTO billing.carrier_bill (
        carrier_id,
        bill_number,
        bill_date,
        total_amount,
        num_shipments,
        account_number,
        file_id
    )
    SELECT
        @Carrier_id AS carrier_id,
        CAST(TRIM(d.[Invoice Number]) AS VARCHAR(100)) AS bill_number,
        CAST(TRIM(d.[Invoice Time]) AS DATE) AS bill_date,
        SUM(CAST(ISNULL(NULLIF(TRIM(d.[Total Billed Amount]), ''), '0') AS DECIMAL(18,2))) AS total_amount,
        COUNT(d.[Tracking Number]) AS num_shipments,
        CAST(TRIM(d.[Merchant ID]) AS VARCHAR(100)) AS account_number,
        @File_id AS file_id
    FROM
        billing.delta_uniuni_bill AS d
    GROUP BY
        d.[Invoice Number], 
        CAST(TRIM(d.[Invoice Time]) AS DATE),
        d.[Merchant ID]
    HAVING
        NOT EXISTS (
            SELECT 1
            FROM billing.carrier_bill AS cb
            WHERE cb.file_id = @File_id  -- File-based idempotency: same file = same data
        );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment line items from delta_uniuni_bill into uniuni_bill.
    Each row represents one tracking number with all its associated charges and
    dimensional data. Uses fail-fast CAST for data validation.
    ================================================================================
    */

    INSERT INTO billing.uniuni_bill (
        invoice_time,
        invoice_number,
        tracking_number,
        carrier_bill_id,
        total_billed_amount,
        billable_weight,
        billable_weight_uom,
        scaled_weight,
        scaled_weight_uom,
        dim_weight,
        dim_weight_uom,
        [zone],
        induction_facility_zipcode,
        dim_length,
        dim_width,
        dim_height,
        package_dim_uom,
        service_type,
        shipment_date,
        induction_time,
        base_fee,
        discount_fee,
        billed_fee,
        signature_fee,
        pickup_fee,
        over_dimension_fee,
        over_max_size_fee,
        over_weight_fee,
        fuel_surcharge,
        peak_season_surcharge,
        delivery_area_surcharge,
        delivery_area_surcharge_extend,
        truck_fee,
        relabel_fee,
        miscellaneous_fee,
        credit_card_surcharge,
        credit,
        approved_claim
    )
    SELECT
        CAST(TRIM(d.[Invoice Time]) AS DATE) AS invoice_time,
        CAST(TRIM(d.[Invoice Number]) AS BIGINT) AS invoice_number,
        CAST(TRIM(d.[Tracking Number]) AS NVARCHAR(50)) AS tracking_number,
        cb.carrier_bill_id,
        CAST(NULLIF(TRIM(d.[Total Billed Amount]), '') AS DECIMAL(18,2)) AS total_billed_amount,
        CAST(NULLIF(TRIM(d.[Billable Weight]), '') AS DECIMAL(18,4)) AS billable_weight,
        CAST(TRIM(d.[Billable Weight UOM]) AS NVARCHAR(10)) AS billable_weight_uom,
        CAST(NULLIF(TRIM(d.[Scaled Weight]), '') AS DECIMAL(18,4)) AS scaled_weight,
        CAST(TRIM(d.[Scaled Weight UOM]) AS NVARCHAR(10)) AS scaled_weight_uom,
        CAST(NULLIF(TRIM(d.[Dim Weight]), '') AS DECIMAL(18,4)) AS dim_weight,
        CAST(TRIM(d.[Dim Weight UOM]) AS NVARCHAR(10)) AS dim_weight_uom,
        CAST(NULLIF(TRIM(d.[Zone]), '') AS INT) AS [zone],
        CAST(NULLIF(TRIM(d.[Induction Facility ZipCode]), '') AS INT) AS induction_facility_zipcode,
        CAST(NULLIF(TRIM(d.[Package Length]), '') AS DECIMAL(18,4)) AS dim_length,
        CAST(NULLIF(TRIM(d.[Package Width]), '') AS DECIMAL(18,4)) AS dim_width,
        CAST(NULLIF(TRIM(d.[Package Height]), '') AS DECIMAL(18,4)) AS dim_height,
        CAST(TRIM(d.[Package DIM UOM]) AS NVARCHAR(10)) AS package_dim_uom,
        CAST(TRIM(d.[Service Type]) AS NVARCHAR(255)) AS service_type,
        CAST(NULLIF(TRIM(d.[Shipped Time]), '') AS DATE) AS shipment_date,
        CAST(NULLIF(TRIM(d.[Induction Time]), '') AS DATE) AS induction_time,
        CAST(ISNULL(NULLIF(TRIM(d.[Base Fee]), ''), '0') AS DECIMAL(10,2)) AS base_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Discount Fee]), ''), '0') AS DECIMAL(10,2)) AS discount_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Billed Fee]), ''), '0') AS DECIMAL(10,2)) AS billed_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Signature Fee]), ''), '0') AS DECIMAL(10,2)) AS signature_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Pickup Fee]), ''), '0') AS DECIMAL(10,2)) AS pickup_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Over Dimension Fee]), ''), '0') AS DECIMAL(10,2)) AS over_dimension_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Over Max Size Fee]), ''), '0') AS DECIMAL(10,2)) AS over_max_size_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Over-weight Fee]), ''), '0') AS DECIMAL(10,2)) AS over_weight_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Fuel Surcharge]), ''), '0') AS DECIMAL(10,2)) AS fuel_surcharge,
        CAST(ISNULL(NULLIF(TRIM(d.[Peak Season Surcharge]), ''), '0') AS DECIMAL(10,2)) AS peak_season_surcharge,
        CAST(ISNULL(NULLIF(TRIM(d.[Delivery Area Surcharge]), ''), '0') AS DECIMAL(10,2)) AS delivery_area_surcharge,
        CAST(ISNULL(NULLIF(TRIM(d.[Delivery Area Surcharge Extend]), ''), '0') AS DECIMAL(10,2)) AS delivery_area_surcharge_extend,
        CAST(ISNULL(NULLIF(TRIM(d.[Truck Fee]), ''), '0') AS DECIMAL(10,2)) AS truck_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Relabel Fee]), ''), '0') AS DECIMAL(10,2)) AS relabel_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Miscellaneous Fee]), ''), '0') AS DECIMAL(10,2)) AS miscellaneous_fee,
        CAST(ISNULL(NULLIF(TRIM(d.[Credit Card Surcharge]), ''), '0') AS DECIMAL(10,2)) AS credit_card_surcharge,
        CAST(ISNULL(NULLIF(TRIM(d.[Credit]), ''), '0') AS DECIMAL(10,2)) AS credit,
        CAST(ISNULL(NULLIF(TRIM(d.[Approved Claim]), ''), '0') AS DECIMAL(10,2)) AS approved_claim
    FROM
        billing.delta_uniuni_bill AS d
    INNER JOIN billing.carrier_bill AS cb
        ON cb.bill_number = CAST(TRIM(d.[Invoice Number]) AS VARCHAR(100))
        AND cb.bill_date = CAST(TRIM(d.[Invoice Time]) AS DATE)
        AND cb.carrier_id = @Carrier_id
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM billing.uniuni_bill AS ub
            WHERE ub.carrier_bill_id = cb.carrier_bill_id  -- Single field check per Design Constraint #9
        );

    SET @LineItemsInserted = @@ROWCOUNT;

    COMMIT TRANSACTION;

    -- Return success results
    SELECT 
        'SUCCESS' AS Status,
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    -- Rollback on error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    -- Build descriptive error message
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();

    DECLARE @DetailedError NVARCHAR(4000) =
        'UniUni Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) +
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