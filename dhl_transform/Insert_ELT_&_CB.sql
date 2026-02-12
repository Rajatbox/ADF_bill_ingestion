/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - @Carrier_id: INT - Carrier identifier from LookupCarrierInfo activity
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of dhl_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process:
         1. Aggregate and insert invoice-level summary data from delta_dhl_bill 
            into carrier_bill - generates carrier_bill_id
         2. Insert line-level billing data from delta_dhl_bill into dhl_bill
            with carrier_bill_id foreign key

Source:   test.delta_dhl_bill
Targets:  Test.carrier_bill (invoice summaries)
          test.dhl_bill (line items)

Tracking Number Logic (applied in Step 2):
  - international_tracking_number: Col 12 saved as-is
  - domestic_tracking_number:      '420' + LEFT(zip, 5) + Col 13 (unique_id)

Carrier Bill Total: SUM(transportation_cost + non_qualified_dimensional_charges
                        + fuel_surcharge_amount + delivery_area_surcharge_amount)
                    Computed from DTL rows (no HDR row in delta table).

Transaction: Both inserts wrapped in transaction for atomicity
Execution Order: SECOND in pipeline (after LookupCarrierInfo.sql)
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
    Aggregates DTL rows by invoice_number and invoice_date to create invoice-level
    summaries in carrier_bill. Total = SUM of 4 charge columns:
    transportation_cost + non_qualified_dimensional_charges
    + fuel_surcharge_amount + delivery_area_surcharge_amount
    
    No HDR row in delta table; total is computed from DTL rows.
    Generates carrier_bill_id values which will be joined in Step 2.
    ================================================================================
    */

    INSERT INTO Test.carrier_bill (
        carrier_id,
        bill_number,
        bill_date,
        total_amount,
        num_shipments
    )
    SELECT
        @Carrier_id AS carrier_id,
        CAST(d.invoice_number AS nvarchar(50)) AS bill_number,
        CAST(NULLIF(TRIM(d.invoice_date), '') AS date) AS bill_date,
        SUM(
            ISNULL(CAST(NULLIF(TRIM(d.transportation_cost), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.non_qualified_dimensional_charges), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.fuel_surcharge_amount), '') AS decimal(18,2)), 0)
            + ISNULL(CAST(NULLIF(TRIM(d.delivery_area_surcharge_amount), '') AS decimal(18,2)), 0)
        ) AS total_amount,
        COUNT(*) AS num_shipments
    FROM
        test.delta_dhl_bill AS d
    WHERE
        d.invoice_number IS NOT NULL
        AND NULLIF(TRIM(d.invoice_date), '') IS NOT NULL
    GROUP BY
        CAST(d.invoice_number AS nvarchar(50)),
        CAST(NULLIF(TRIM(d.invoice_date), '') AS date)
    HAVING
        NOT EXISTS (
            SELECT 1
            FROM Test.carrier_bill AS cb
            WHERE cb.bill_number = CAST(d.invoice_number AS nvarchar(50))
                AND cb.bill_date = CAST(NULLIF(TRIM(d.invoice_date), '') AS date)
                AND cb.carrier_id = @Carrier_id
        );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Each DTL row represents one shipment. Inserts into dhl_bill with:
    - carrier_bill_id FK from Step 1
    - international_tracking_number: Col 12 as-is
    - domestic_tracking_number: '420' + first 5 digits of zip + unique_id (Col 13)
    - All 4 charge columns preserved for unified layer
    - Column order matches real bill sequence
    
    NOT EXISTS check uses carrier_bill_id (Design Constraint #4).
    ================================================================================
    */

    INSERT INTO test.dhl_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        shipping_date,
        international_tracking_number,
        domestic_tracking_number,
        recipient_zip_postal_code,
        recipient_country,
        shipping_method,
        shipped_weight,
        shipped_weight_unit,
        billed_weight,
        billed_weight_unit,
        [zone],
        transportation_cost,
        non_qualified_dimensional_charges,
        fuel_surcharge_amount,
        delivery_area_surcharge_amount
    )
    SELECT 
        cb.carrier_bill_id,
        CAST(d.invoice_number AS nvarchar(50)) AS invoice_number,
        CAST(NULLIF(TRIM(d.invoice_date), '') AS date) AS invoice_date,
        CAST(NULLIF(TRIM(d.shipping_date), '') AS date) AS shipping_date,
        -- Col 12: International tracking number as-is
        CAST(d.international_tracking_number AS nvarchar(255)),
        -- Col 13: Domestic tracking = '420' + first 5 digits of zip + unique_id
        '420' + LEFT(REPLACE(CAST(d.recipient_zip_postal_code AS varchar(50)), ' ', ''), 5)
             + CAST(d.domestic_tracking_number AS varchar(255)) AS domestic_tracking_number,
        CAST(d.recipient_zip_postal_code AS nvarchar(255)),
        CAST(d.recipient_country AS nvarchar(10)),
        CAST(d.shipping_method AS nvarchar(350)),
        CAST(NULLIF(TRIM(d.shipped_weight), '') AS decimal(18,2)),
        CAST(d.shipped_weight_unit_of_measure AS nvarchar(10)),
        CAST(NULLIF(TRIM(d.billed_weight), '') AS decimal(18,2)),
        CAST(d.billed_weight_unit_of_measure AS nvarchar(10)),
        CAST(d.[zone] AS nvarchar(255)),
        CAST(NULLIF(TRIM(d.transportation_cost), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.non_qualified_dimensional_charges), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.fuel_surcharge_amount), '') AS decimal(18,2)),
        CAST(NULLIF(TRIM(d.delivery_area_surcharge_amount), '') AS decimal(18,2))
    FROM test.delta_dhl_bill d
    INNER JOIN Test.carrier_bill cb
        ON cb.bill_number = CAST(d.invoice_number AS nvarchar(50))
        AND cb.bill_date = CAST(NULLIF(TRIM(d.invoice_date), '') AS date)
        AND cb.carrier_id = @Carrier_id
    WHERE d.invoice_number IS NOT NULL
      AND NULLIF(TRIM(d.invoice_date), '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 
          FROM test.dhl_bill t
          WHERE t.carrier_bill_id = cb.carrier_bill_id
      );

    SET @LineItemsInserted = @@ROWCOUNT;

    -- Commit transaction if all succeeds
    COMMIT TRANSACTION;
    
    SELECT 
        'SUCCESS' AS Status, 
        @InvoicesInserted AS InvoicesInserted,
        @LineItemsInserted AS LineItemsInserted;

END TRY
BEGIN CATCH
    -- Rollback transaction on any error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    -- Build descriptive error message
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorNumber INT = ERROR_NUMBER();
    
    DECLARE @DetailedError NVARCHAR(4000) = 
        'DHL Insert_ELT_&_CB.sql failed at line ' + CAST(@ErrorLine AS NVARCHAR(10)) + 
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
