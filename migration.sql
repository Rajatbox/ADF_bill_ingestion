/*
================================================================================
Migration: ADF Bill Ingestion Schema Updates
================================================================================
Phase 1 — Promote resolution fields into silver bill tables (DHL, FedEx)
Phase 2 — Speedship aggregator staging and silver tables

Idempotent: guarded by sys.columns / OBJECT_ID existence checks.
================================================================================
*/

SET NOCOUNT ON;

-- ============================================================
-- Phase 1: billing.dhl_bill
-- ============================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.dhl_bill') AND name = 'billing_ref1'
)
BEGIN
    ALTER TABLE billing.dhl_bill ADD billing_ref1 NVARCHAR(255) NULL;
    PRINT 'Added billing.dhl_bill.billing_ref1';
END
ELSE
    PRINT 'billing.dhl_bill.billing_ref1 already exists — skipped';

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.dhl_bill') AND name = 'bol_number'
)
BEGIN
    ALTER TABLE billing.dhl_bill ADD bol_number NVARCHAR(255) NULL;
    PRINT 'Added billing.dhl_bill.bol_number';
END
ELSE
    PRINT 'billing.dhl_bill.bol_number already exists — skipped';

-- ============================================================
-- Phase 1: billing.fedex_bill
-- ============================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('billing.fedex_bill') AND name = 'original_customer_reference'
)
BEGIN
    ALTER TABLE billing.fedex_bill ADD original_customer_reference NVARCHAR(255) NULL;
    PRINT 'Added billing.fedex_bill.original_customer_reference';
END
ELSE
    PRINT 'billing.fedex_bill.original_customer_reference already exists — skipped';

-- ============================================================
-- Phase 2: Speedship delta + silver tables
-- ============================================================

IF OBJECT_ID('billing.delta_speedship_bill', 'U') IS NULL
BEGIN
    CREATE TABLE billing.delta_speedship_bill (
        [Customer #] VARCHAR(255) NULL,
        [Invoice #] VARCHAR(255) NULL,
        [Line of Business] VARCHAR(255) NULL,
        [Airbill #] VARCHAR(255) NULL,
        [Ship date] VARCHAR(255) NULL,
        [PRO #] VARCHAR(255) NULL,
        [BOL #] VARCHAR(255) NULL,
        [SCAC] VARCHAR(255) NULL,
        [Bill Type] VARCHAR(255) NULL,
        [Shippers Name] VARCHAR(255) NULL,
        [Shippers Address 1] VARCHAR(255) NULL,
        [Shippers Address 2] VARCHAR(255) NULL,
        [Shippers Address 3] VARCHAR(255) NULL,
        [Shippers City] VARCHAR(255) NULL,
        [Shippers State] VARCHAR(255) NULL,
        [Shippers ZIP] VARCHAR(255) NULL,
        [Receiver Name] VARCHAR(255) NULL,
        [Receiver Address 1] VARCHAR(255) NULL,
        [Receiver Address 2] VARCHAR(255) NULL,
        [Receiver Address 3] VARCHAR(255) NULL,
        [Receiver City] VARCHAR(255) NULL,
        [Receiver State] VARCHAR(255) NULL,
        [Receiver ZIP] VARCHAR(255) NULL,
        [Consignee Name] VARCHAR(255) NULL,
        [Consignee City] VARCHAR(255) NULL,
        [Consignee State] VARCHAR(255) NULL,
        [Consignee Zip] VARCHAR(255) NULL,
        [Originating Customer] VARCHAR(255) NULL,
        [Customer Name] VARCHAR(255) NULL,
        [Customer Address 1] VARCHAR(255) NULL,
        [Customer Address 2] VARCHAR(255) NULL,
        [Customer City] VARCHAR(255) NULL,
        [Customer State] VARCHAR(255) NULL,
        [Customer ZIP] VARCHAR(255) NULL,
        [Handling Unit] VARCHAR(255) NULL,
        [Pieces] VARCHAR(255) NULL,
        [Original Weight] VARCHAR(255) NULL,
        [Charged Weight] VARCHAR(255) NULL,
        [Class] VARCHAR(255) NULL,
        [Charge Type 1] VARCHAR(255) NULL,
        [Charge Amount 1] VARCHAR(255) NULL,
        [Charge Type 2] VARCHAR(255) NULL,
        [Charge Amount 2] VARCHAR(255) NULL,
        [Charge Type 3] VARCHAR(255) NULL,
        [Charge Amount 3] VARCHAR(255) NULL,
        [Charge Type 4] VARCHAR(255) NULL,
        [Charge Amount 4] VARCHAR(255) NULL,
        [Charge Type 5] VARCHAR(255) NULL,
        [Charge Amount 5] VARCHAR(255) NULL,
        [Charge Type 6] VARCHAR(255) NULL,
        [Charge Amount 6] VARCHAR(255) NULL,
        [Charge Type 7] VARCHAR(255) NULL,
        [Charge Amount 7] VARCHAR(255) NULL,
        [Charge Type 8] VARCHAR(255) NULL,
        [Charge Amount 8] VARCHAR(255) NULL,
        [Charge Total] VARCHAR(255) NULL,
        [Invoice Date] VARCHAR(255) NULL,
        [Billing Reference 1] VARCHAR(255) NULL,
        [Billing Reference 2] VARCHAR(255) NULL,
        [Vendor Reference 1] VARCHAR(255) NULL,
        [Vendor Reference 2] VARCHAR(255) NULL,
        [Sent By] VARCHAR(255) NULL,
        [Service level] VARCHAR(255) NULL,
        [ Zone] VARCHAR(255) NULL,
        [You Owe As] VARCHAR(255) NULL,
        [Description1] VARCHAR(255) NULL,
        [Description2] VARCHAR(255) NULL,
        [Description3] VARCHAR(255) NULL,
        [Description4] VARCHAR(255) NULL,
        [Pickuplocation] VARCHAR(255) NULL,
        [SenderNo] VARCHAR(255) NULL,
        [ReceiverNo] VARCHAR(255) NULL,
        [ReceiverLine1] VARCHAR(255) NULL,
        [ReceiverLine2] VARCHAR(255) NULL,
        [Package Reference 1] VARCHAR(255) NULL,
        [Package Reference 2] VARCHAR(255) NULL,
        [Package Reference 3] VARCHAR(255) NULL,
        [Package Reference 4] VARCHAR(255) NULL,
        [Package Reference 5] VARCHAR(255) NULL,
        [Package Reference 6] VARCHAR(255) NULL,
        [Package Reference 7] VARCHAR(255) NULL,
        [Package Reference 8] VARCHAR(255) NULL,
        [UPS #] VARCHAR(255) NULL
    );
    PRINT 'Created billing.delta_speedship_bill';
END
ELSE
    PRINT 'billing.delta_speedship_bill already exists — skipped';

IF OBJECT_ID('billing.speedship_bill', 'U') IS NULL
BEGIN
    CREATE TABLE billing.speedship_bill (
        id INT IDENTITY(1,1) NOT NULL,
        carrier_bill_id INT NULL,
        invoice_number NVARCHAR(100) NOT NULL,
        invoice_date DATE NOT NULL,
        customer_number NVARCHAR(100) NULL,
        line_of_business NVARCHAR(50) NULL,
        tracking_number NVARCHAR(255) NOT NULL,
        shipment_date DATE NULL,
        scac NVARCHAR(255) NULL,
        integrated_carrier NVARCHAR(100) NULL,
        bill_type NVARCHAR(50) NULL,
        service_level NVARCHAR(255) NULL,
        destination_zone NVARCHAR(50) NULL,
        charged_weight DECIMAL(18,6) NULL,
        weight_unit NVARCHAR(10) NULL,
        charge_total DECIMAL(18,2) NULL,
        charge_type_1 NVARCHAR(255) NULL,
        charge_amount_1 DECIMAL(18,2) NULL,
        charge_type_2 NVARCHAR(255) NULL,
        charge_amount_2 DECIMAL(18,2) NULL,
        charge_type_3 NVARCHAR(255) NULL,
        charge_amount_3 DECIMAL(18,2) NULL,
        charge_type_4 NVARCHAR(255) NULL,
        charge_amount_4 DECIMAL(18,2) NULL,
        charge_type_5 NVARCHAR(255) NULL,
        charge_amount_5 DECIMAL(18,2) NULL,
        charge_type_6 NVARCHAR(255) NULL,
        charge_amount_6 DECIMAL(18,2) NULL,
        charge_type_7 NVARCHAR(255) NULL,
        charge_amount_7 DECIMAL(18,2) NULL,
        charge_type_8 NVARCHAR(255) NULL,
        charge_amount_8 DECIMAL(18,2) NULL,
        billing_reference_1 NVARCHAR(255) NULL,
        billing_reference_2 NVARCHAR(255) NULL,
        customer_name NVARCHAR(255) NULL,
        created_date DATETIME2 DEFAULT SYSDATETIME() NOT NULL,

        CONSTRAINT PK_speedship_bill PRIMARY KEY (id),
        CONSTRAINT FK_speedship_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
            REFERENCES billing.carrier_bill(carrier_bill_id)
    );

    CREATE NONCLUSTERED INDEX IX_speedship_bill_carrier_bill_id
        ON billing.speedship_bill (carrier_bill_id);

    CREATE NONCLUSTERED INDEX IX_speedship_bill_created_date
        ON billing.speedship_bill (created_date);

    CREATE NONCLUSTERED INDEX IX_speedship_bill_tracking_number_invoice
        ON billing.speedship_bill (tracking_number, invoice_number, invoice_date);

    PRINT 'Created billing.speedship_bill and indexes';
END
ELSE
    PRINT 'billing.speedship_bill already exists — skipped';

PRINT 'Migration complete.';
