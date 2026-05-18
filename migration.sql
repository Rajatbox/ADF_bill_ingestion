/*
================================================================================
Migration: Add USPS Modern carrier tables
================================================================================
Purpose: Creates staging (delta) and normalized (carrier-specific) tables
         for the USPS Modern billing integration.

Source:  ShipHero label API export — one row per shipment, pre-filtered to
         USPS Modern. 44 snake_case columns.

New tables:
  - billing.delta_usps_modern_bill   (bronze / staging)
  - billing.usps_modern_bill         (silver / normalized)

Affected scripts (new in this release):
  - usps_modern_transform/Insert_ELT_&_CB.sql
  - usps_modern_transform/Sync_Reference_Data.sql
  - usps_modern_transform/Insert_Unified_tables.sql

Idempotent: DROP IF EXISTS before each CREATE — safe to rerun.
================================================================================
*/

SET NOCOUNT ON;

-- ============================================================
-- DELTA TABLE (bronze)
-- ============================================================

IF OBJECT_ID('billing.delta_usps_modern_bill', 'U') IS NOT NULL
    DROP TABLE billing.delta_usps_modern_bill;

CREATE TABLE billing.delta_usps_modern_bill (
    shipment_order_id       VARCHAR(255) NULL,
    shipment_created_date   VARCHAR(255) NULL,
    label_id                VARCHAR(255) NULL,
    label_legacy_id         VARCHAR(255) NULL,
    account_id              VARCHAR(255) NULL,
    shipment_id             VARCHAR(255) NULL,
    order_id                VARCHAR(255) NULL,
    box_id                  VARCHAR(255) NULL,
    box_name                VARCHAR(255) NULL,
    status                  VARCHAR(255) NULL,
    tracking_number         VARCHAR(255) NULL,
    alternate_tracking_id   VARCHAR(255) NULL,
    order_number            VARCHAR(255) NULL,
    order_account_id        VARCHAR(255) NULL,
    carrier                 VARCHAR(255) NULL,
    shipping_name           VARCHAR(MAX) NULL,
    shipping_method         VARCHAR(255) NULL,
    cost                    VARCHAR(255) NULL,
    box_code                VARCHAR(255) NULL,
    device_id               VARCHAR(255) NULL,
    delivered               VARCHAR(255) NULL,
    picked_up               VARCHAR(255) NULL,
    refunded                VARCHAR(255) NULL,
    needs_refund            VARCHAR(255) NULL,
    profile                 VARCHAR(255) NULL,
    partner_fulfillment_id  VARCHAR(255) NULL,
    full_size_to_print      VARCHAR(MAX) NULL,
    packing_slip            VARCHAR(255) NULL,
    warehouse               VARCHAR(255) NULL,
    warehouse_id            VARCHAR(255) NULL,
    insurance_amount        VARCHAR(255) NULL,
    carrier_account_id      VARCHAR(255) NULL,
    source                  VARCHAR(255) NULL,
    label_created_date      VARCHAR(255) NULL,
    tracking_url            VARCHAR(MAX) NULL,
    package_number          VARCHAR(255) NULL,
    parcelview_url          VARCHAR(MAX) NULL,
    tracking_status         VARCHAR(255) NULL,
    in_shipping_container   VARCHAR(255) NULL,
    shipping_container_id   VARCHAR(255) NULL,
    dim_weight              VARCHAR(255) NULL,  -- e.g. "1.5562 lb"  → parsed + converted to OZ in ELT
    dim_height              VARCHAR(255) NULL,  -- e.g. "1.0000 inch"
    dim_width               VARCHAR(255) NULL,  -- e.g. "12.00 inch"
    dim_length              VARCHAR(255) NULL   -- e.g. "20.00 inch"
);

PRINT 'Created billing.delta_usps_modern_bill';

-- ============================================================
-- CARRIER-SPECIFIC TABLE (silver)
-- Indexes are dropped automatically with the table.
-- ============================================================

IF OBJECT_ID('billing.usps_modern_bill', 'U') IS NOT NULL
    DROP TABLE billing.usps_modern_bill;

CREATE TABLE billing.usps_modern_bill (
    id                      INT IDENTITY(1,1)   NOT NULL,
    carrier_bill_id         INT                 NULL,
    invoice_number          NVARCHAR(100)       NOT NULL,
    invoice_date            DATE                NOT NULL,
    tracking_number         NVARCHAR(255)       NOT NULL,
    label_id                NVARCHAR(255)       NULL,
    label_legacy_id         NVARCHAR(255)       NULL,
    order_number            NVARCHAR(255)       NULL,
    order_account_id        NVARCHAR(255)       NULL,
    shipment_created_date   DATETIME2           NULL,
    label_created_date      DATETIME2           NULL,   -- used as ship date
    shipping_method         NVARCHAR(255)       NULL,
    billed_weight_lb        DECIMAL(18,4)       NULL,   -- raw LB; converted × 16 to OZ in unified layer
    billed_height_in        DECIMAL(18,4)       NULL,
    billed_length_in        DECIMAL(18,4)       NULL,
    billed_width_in         DECIMAL(18,4)       NULL,
    cost                    DECIMAL(18,2)       NULL,   -- "Freight charge" amount
    status                  NVARCHAR(50)        NULL,
    box_name                NVARCHAR(255)       NULL,
    carrier_account_id      NVARCHAR(255)       NULL,
    warehouse               NVARCHAR(255)       NULL,
    created_date            DATETIME2           DEFAULT sysdatetime() NOT NULL,

    CONSTRAINT PK_usps_modern_bill PRIMARY KEY (id),
    CONSTRAINT FK_usps_modern_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
        REFERENCES billing.carrier_bill(carrier_bill_id)
);

CREATE NONCLUSTERED INDEX IX_usps_modern_bill_carrier_bill_id
ON billing.usps_modern_bill (carrier_bill_id);

CREATE NONCLUSTERED INDEX IX_usps_modern_bill_tracking_number
ON billing.usps_modern_bill (tracking_number, invoice_number, invoice_date);

PRINT 'Created billing.usps_modern_bill';

PRINT 'Migration complete: USPS Modern tables ready.';
