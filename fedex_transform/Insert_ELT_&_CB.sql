/*
================================================================================
Insert Script: ELT & Carrier Bill (CB) - Transactional
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.

ADF Pipeline Variables Required:
  INPUT:
    - None (reads from delta_fedex_bill staging table populated by ADF Copy activity)
  
  OUTPUT (Query Results):
    - Status: 'SUCCESS' or 'ERROR'
    - InvoicesInserted: INT - Number of carrier_bill records inserted
    - LineItemsInserted: INT - Number of fedex_bill line items inserted
    - ErrorNumber: INT (if error)
    - ErrorMessage: NVARCHAR (if error)

Purpose: Two-step transactional data insertion process:
         1. Aggregate and insert invoice-level summary data from delta_fedex_bill 
            into carrier_bill (Carrier Bill summary) - generates carrier_bill_id
         2. Insert line-level billing data from delta_fedex_bill (ELT staging) 
            into fedex_bill (Carrier Bill line items) with carrier_bill_id foreign key

Source:   test.delta_fedex_bill
Targets:  Test.carrier_bill (invoice summaries)
Test.fedex_bill (line items)

Validation: Fails if invoice_date or shipment_date is NULL or empty
Match:      invoice_number AND invoice_date (INSERT WHERE NOT EXISTS)
Transaction: Both inserts wrapped in transaction for atomicity - all succeed or all fail

Execution Order: SECOND in pipeline (after LookupCarrierInfo.sql)
================================================================================
*/

/*
================================================================================
Tech Debt
================================================================================
TODO: For service_type column, use COALESCE on [Ground Service] and [Service Type] 
      from delta_fedex_bill to handle cases where one may be NULL.
      Example: COALESCE(d.[Ground Service], d.[Service Type]) AS service_type
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
    Aggregates line items by invoice_number and invoice_date to create invoice-level
    summaries in carrier_bill. Calculates total_amount (sum of net_charge_amount) and
    num_shipments (count of all line items) per invoice.
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
        CAST(d.[Invoice Number] AS nvarchar(50)) AS bill_number,
        CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date) AS bill_date,
        SUM(TRY_CAST(REPLACE(d.[Net Charge Amount], ',', '') AS decimal(18,2))) AS total_amount,
        COUNT(*) AS num_shipments
    FROM
test.delta_fedex_bill AS d
    WHERE
        d.[Invoice Number] IS NOT NULL
        AND NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') IS NOT NULL
        -- Note: No tracking ID filter here - invoice aggregation should include ALL line items
        -- Tracking ID validation happens in Step 2 (line-level insert)
    GROUP BY
        CAST(d.[Invoice Number] AS nvarchar(50)),
        CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date)
    HAVING
        NOT EXISTS (
            SELECT 1
            FROM Test.carrier_bill AS cb
            WHERE cb.bill_number = CAST(d.[Invoice Number] AS nvarchar(50))
                AND cb.bill_date = CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date)
        );

    SET @InvoicesInserted = @@ROWCOUNT;

    /*
    ================================================================================
    Step 2: Insert Line-Level Billing Data
    ================================================================================
    Inserts individual shipment/line items from delta_fedex_bill into fedex_bill.
    Each row represents a single shipment with tracking details, charges, and dimensions.
    Joins directly with carrier_bill table to get carrier_bill_id for referential integrity.
    NOT EXISTS check includes carrier_bill_id for batch-specific duplicate detection.
    ================================================================================
    */

    INSERT INTO Test.fedex_bill (
        carrier_bill_id,
        invoice_number,
        invoice_date,
        service_type,
        shipment_date,
        express_or_ground_tracking_id,
        msp_tracking_id,
        zone_code,
        net_charge_amount,
        rated_weight_amount,
        rated_weight_units,
        dim_length,
        dim_width,
        dim_height,
        dim_unit,
        shipper_zip_code,
        [Transportation Charge Amount],
    [Tracking ID Charge Description],
    [Tracking ID Charge Amount],
    [Tracking ID Charge Description_1],
    [Tracking ID Charge Amount_1],
    [Tracking ID Charge Description_2],
    [Tracking ID Charge Amount_2],
    [Tracking ID Charge Description_3],
    [Tracking ID Charge Amount_3],
    [Tracking ID Charge Description_4],
    [Tracking ID Charge Amount_4],
    [Tracking ID Charge Description_5],
    [Tracking ID Charge Amount_5],
    [Tracking ID Charge Description_6],
    [Tracking ID Charge Amount_6],
    [Tracking ID Charge Description_7],
    [Tracking ID Charge Amount_7],
    [Tracking ID Charge Description_8],
    [Tracking ID Charge Amount_8],
    [Tracking ID Charge Description_9],
    [Tracking ID Charge Amount_9],
    [Tracking ID Charge Description_10],
    [Tracking ID Charge Amount_10],
    [Tracking ID Charge Description_11],
    [Tracking ID Charge Amount_11],
    [Tracking ID Charge Description_12],
    [Tracking ID Charge Amount_12],
    [Tracking ID Charge Description_13],
    [Tracking ID Charge Amount_13],
    [Tracking ID Charge Description_14],
    [Tracking ID Charge Amount_14],
    [Tracking ID Charge Description_15],
    [Tracking ID Charge Amount_15],
    [Tracking ID Charge Description_16],
    [Tracking ID Charge Amount_16],
    [Tracking ID Charge Description_17],
    [Tracking ID Charge Amount_17],
    [Tracking ID Charge Description_18],
    [Tracking ID Charge Amount_18],
    [Tracking ID Charge Description_19],
    [Tracking ID Charge Amount_19],
    [Tracking ID Charge Description_20],
    [Tracking ID Charge Amount_20],
    [Tracking ID Charge Description_21],
    [Tracking ID Charge Amount_21],
    [Tracking ID Charge Description_22],
    [Tracking ID Charge Amount_22],
    [Tracking ID Charge Description_23],
    [Tracking ID Charge Amount_23],
    [Tracking ID Charge Description_24],
    [Tracking ID Charge Amount_24],
    [Tracking ID Charge Description_25],
    [Tracking ID Charge Amount_25],
    [Tracking ID Charge Description_26],
    [Tracking ID Charge Amount_26],
    [Tracking ID Charge Description_27],
    [Tracking ID Charge Amount_27],
    [Tracking ID Charge Description_28],
    [Tracking ID Charge Amount_28],
    [Tracking ID Charge Description_29],
    [Tracking ID Charge Amount_29],
    [Tracking ID Charge Description_30],
    [Tracking ID Charge Amount_30],
    [Tracking ID Charge Description_31],
    [Tracking ID Charge Amount_31],
    [Tracking ID Charge Description_32],
    [Tracking ID Charge Amount_32],
    [Tracking ID Charge Description_33],
    [Tracking ID Charge Amount_33],
    [Tracking ID Charge Description_34],
    [Tracking ID Charge Amount_34],
    [Tracking ID Charge Description_35],
    [Tracking ID Charge Amount_35],
    [Tracking ID Charge Description_36],
    [Tracking ID Charge Amount_36],
    [Tracking ID Charge Description_37],
    [Tracking ID Charge Amount_37],
    [Tracking ID Charge Description_38],
    [Tracking ID Charge Amount_38],
    [Tracking ID Charge Description_39],
    [Tracking ID Charge Amount_39],
    [Tracking ID Charge Description_40],
    [Tracking ID Charge Amount_40],
    [Tracking ID Charge Description_41],
    [Tracking ID Charge Amount_41],
    [Tracking ID Charge Description_42],
    [Tracking ID Charge Amount_42],
    [Tracking ID Charge Description_43],
    [Tracking ID Charge Amount_43],
    [Tracking ID Charge Description_44],
    [Tracking ID Charge Amount_44],
    [Tracking ID Charge Description_45],
    [Tracking ID Charge Amount_45],
    [Tracking ID Charge Description_46],
    [Tracking ID Charge Amount_46],
    [Tracking ID Charge Description_47],
    [Tracking ID Charge Amount_47],
    [Tracking ID Charge Description_48],
    [Tracking ID Charge Amount_48],
    [Tracking ID Charge Description_49],
    [Tracking ID Charge Amount_49],
    [Tracking ID Charge Description_50],
    [Tracking ID Charge Amount_50]
)
SELECT 
    cb.carrier_bill_id,
    CAST(d.[Invoice Number] AS nvarchar(50)) AS invoice_number,
    CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date) AS invoice_date,
    CAST(d.[Service Type] AS nvarchar(255)) AS service_type,
    CAST(NULLIF(TRIM(CAST(d.[Shipment Date] AS varchar)), '') AS date) AS shipment_date,
    CAST(d.[Express or Ground Tracking ID] AS nvarchar(255)) AS express_or_ground_tracking_id,
    CAST(d.[MPS Package ID] AS nvarchar(255)) AS msp_tracking_id,
    CAST(d.[Zone Code] AS nvarchar(255)) AS zone_code,
    CAST(REPLACE(d.[Net Charge Amount], ',', '') AS decimal(18,2)) AS net_charge_amount,
    CAST(d.[Rated Weight Amount] AS decimal(18,2)) AS rated_weight_amount,
    CAST(d.[Rated Weight Units] AS nvarchar(10)) AS rated_weight_units,
    CAST(d.[Dim Length] AS decimal(18,2)) AS dim_length,
    CAST(d.[Dim Width] AS decimal(18,2)) AS dim_width,
    CAST(d.[Dim Height] AS decimal(18,2)) AS dim_height,
    CAST(d.[Dim Unit] AS nvarchar(9)) AS dim_unit,
    CAST(d.[Shipper Zip Code] AS nvarchar(100)) AS shipper_zip_code,
    CAST(REPLACE(d.[Transportation Charge Amount], ',', '') AS decimal(18,2)) AS [Transportation Charge Amount],
    d.[Tracking ID Charge Description],
    CAST(REPLACE(d.[Tracking ID Charge Amount], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount],
    d.[Tracking ID Charge Description_1],
    CAST(REPLACE(d.[Tracking ID Charge Amount_1], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_1],
    d.[Tracking ID Charge Description_2],
    CAST(REPLACE(d.[Tracking ID Charge Amount_2], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_2],
    d.[Tracking ID Charge Description_3],
    CAST(REPLACE(d.[Tracking ID Charge Amount_3], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_3],
    d.[Tracking ID Charge Description_4],
    CAST(REPLACE(d.[Tracking ID Charge Amount_4], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_4],
    d.[Tracking ID Charge Description_5],
    CAST(REPLACE(d.[Tracking ID Charge Amount_5], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_5],
    d.[Tracking ID Charge Description_6],
    CAST(REPLACE(d.[Tracking ID Charge Amount_6], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_6],
    d.[Tracking ID Charge Description_7],
    CAST(REPLACE(d.[Tracking ID Charge Amount_7], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_7],
    d.[Tracking ID Charge Description_8],
    CAST(REPLACE(d.[Tracking ID Charge Amount_8], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_8],
    d.[Tracking ID Charge Description_9],
    CAST(REPLACE(d.[Tracking ID Charge Amount_9], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_9],
    d.[Tracking ID Charge Description_10],
    CAST(REPLACE(d.[Tracking ID Charge Amount_10], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_10],
    d.[Tracking ID Charge Description_11],
    CAST(REPLACE(d.[Tracking ID Charge Amount_11], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_11],
    d.[Tracking ID Charge Description_12],
    CAST(REPLACE(d.[Tracking ID Charge Amount_12], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_12],
    d.[Tracking ID Charge Description_13],
    CAST(REPLACE(d.[Tracking ID Charge Amount_13], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_13],
    d.[Tracking ID Charge Description_14],
    CAST(REPLACE(d.[Tracking ID Charge Amount_14], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_14],
    d.[Tracking ID Charge Description_15],
    CAST(REPLACE(d.[Tracking ID Charge Amount_15], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_15],
    d.[Tracking ID Charge Description_16],
    CAST(REPLACE(d.[Tracking ID Charge Amount_16], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_16],
    d.[Tracking ID Charge Description_17],
    CAST(REPLACE(d.[Tracking ID Charge Amount_17], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_17],
    d.[Tracking ID Charge Description_18],
    CAST(REPLACE(d.[Tracking ID Charge Amount_18], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_18],
    d.[Tracking ID Charge Description_19],
    CAST(REPLACE(d.[Tracking ID Charge Amount_19], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_19],
    d.[Tracking ID Charge Description_20],
    CAST(REPLACE(d.[Tracking ID Charge Amount_20], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_20],
    d.[Tracking ID Charge Description_21],
    CAST(REPLACE(d.[Tracking ID Charge Amount_21], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_21],
    d.[Tracking ID Charge Description_22],
    CAST(REPLACE(d.[Tracking ID Charge Amount_22], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_22],
    d.[Tracking ID Charge Description_23],
    CAST(REPLACE(d.[Tracking ID Charge Amount_23], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_23],
    d.[Tracking ID Charge Description_24],
    CAST(REPLACE(d.[Tracking ID Charge Amount_24], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_24],
    d.[Tracking ID Charge Description_25],
    CAST(REPLACE(d.[Tracking ID Charge Amount_25], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_25],
    d.[Tracking ID Charge Description_26],
    CAST(REPLACE(d.[Tracking ID Charge Amount_26], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_26],
    d.[Tracking ID Charge Description_27],
    CAST(REPLACE(d.[Tracking ID Charge Amount_27], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_27],
    d.[Tracking ID Charge Description_28],
    CAST(REPLACE(d.[Tracking ID Charge Amount_28], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_28],
    d.[Tracking ID Charge Description_29],
    CAST(REPLACE(d.[Tracking ID Charge Amount_29], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_29],
    d.[Tracking ID Charge Description_30],
    CAST(REPLACE(d.[Tracking ID Charge Amount_30], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_30],
    d.[Tracking ID Charge Description_31],
    CAST(REPLACE(d.[Tracking ID Charge Amount_31], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_31],
    d.[Tracking ID Charge Description_32],
    CAST(REPLACE(d.[Tracking ID Charge Amount_32], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_32],
    d.[Tracking ID Charge Description_33],
    CAST(REPLACE(d.[Tracking ID Charge Amount_33], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_33],
    d.[Tracking ID Charge Description_34],
    CAST(REPLACE(d.[Tracking ID Charge Amount_34], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_34],
    d.[Tracking ID Charge Description_35],
    CAST(REPLACE(d.[Tracking ID Charge Amount_35], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_35],
    d.[Tracking ID Charge Description_36],
    CAST(REPLACE(d.[Tracking ID Charge Amount_36], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_36],
    d.[Tracking ID Charge Description_37],
    CAST(REPLACE(d.[Tracking ID Charge Amount_37], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_37],
    d.[Tracking ID Charge Description_38],
    CAST(REPLACE(d.[Tracking ID Charge Amount_38], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_38],
    d.[Tracking ID Charge Description_39],
    CAST(REPLACE(d.[Tracking ID Charge Amount_39], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_39],
    d.[Tracking ID Charge Description_40],
    CAST(REPLACE(d.[Tracking ID Charge Amount_40], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_40],
    d.[Tracking ID Charge Description_41],
    CAST(REPLACE(d.[Tracking ID Charge Amount_41], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_41],
    d.[Tracking ID Charge Description_42],
    CAST(REPLACE(d.[Tracking ID Charge Amount_42], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_42],
    d.[Tracking ID Charge Description_43],
    CAST(REPLACE(d.[Tracking ID Charge Amount_43], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_43],
    d.[Tracking ID Charge Description_44],
    CAST(REPLACE(d.[Tracking ID Charge Amount_44], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_44],
    d.[Tracking ID Charge Description_45],
    CAST(REPLACE(d.[Tracking ID Charge Amount_45], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_45],
    d.[Tracking ID Charge Description_46],
    CAST(REPLACE(d.[Tracking ID Charge Amount_46], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_46],
    d.[Tracking ID Charge Description_47],
    CAST(REPLACE(d.[Tracking ID Charge Amount_47], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_47],
    d.[Tracking ID Charge Description_48],
    CAST(REPLACE(d.[Tracking ID Charge Amount_48], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_48],
    d.[Tracking ID Charge Description_49],
    CAST(REPLACE(d.[Tracking ID Charge Amount_49], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_49],
    d.[Tracking ID Charge Description_50],
    CAST(REPLACE(d.[Tracking ID Charge Amount_50], ',', '') AS decimal(18,2)) AS [Tracking ID Charge Amount_50]
FROM test.delta_fedex_bill d
INNER JOIN Test.carrier_bill cb
    ON cb.bill_number = CAST(d.[Invoice Number] AS nvarchar(50))
    AND cb.bill_date = CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date)
WHERE d.[Invoice Number] IS NOT NULL
  AND NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM Test.fedex_bill t
      WHERE t.invoice_number = CAST(d.[Invoice Number] AS nvarchar(50))
        AND t.invoice_date = CAST(NULLIF(TRIM(CAST(d.[Invoice Date] AS varchar)), '') AS date)
        AND t.carrier_bill_id = cb.carrier_bill_id
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
    
    -- Return error details
    SELECT 
        'ERROR' AS Status,
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_MESSAGE() AS ErrorMessage,
        ERROR_LINE() AS ErrorLine;
    
    -- Re-throw error for ADF to handle
    THROW;
END CATCH;
