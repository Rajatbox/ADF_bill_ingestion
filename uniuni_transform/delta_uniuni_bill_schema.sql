/*
================================================================================
Delta Table Schema: UniUni Billing
================================================================================
Note: Database name is parameterized via ADF Linked Service per environment.
      This staging table has a 1:1 mapping with the UniUni CSV file structure.

Purpose: Staging table for raw UniUni billing CSV data
Source:  UniUni billing CSV files (uploaded via ADF Copy activity)
Format:  Wide format (15 charge columns)
================================================================================
*/

CREATE TABLE test.delta_uniuni_bill (
    [Document Number] VARCHAR(255) NULL,
    [Invoice Number] VARCHAR(255) NULL,
    [Invoice Time] VARCHAR(255) NULL,
    [GUID] VARCHAR(255) NULL,
    [Merchant Reference Number] VARCHAR(255) NULL,
    [Tracking Number] VARCHAR(255) NULL,
    [Parent Merchant ID] VARCHAR(255) NULL,
    [Merchant ID] VARCHAR(255) NULL,
    [Merchant Name] VARCHAR(255) NULL,
    [Currency] VARCHAR(255) NULL,
    [Base Fee] VARCHAR(255) NULL,
    [Discount Fee] VARCHAR(255) NULL,
    [Discount Percentage] VARCHAR(255) NULL,
    [Billed Fee] VARCHAR(255) NULL,
    [Signature Fee] VARCHAR(255) NULL,
    [Pickup Fee] VARCHAR(255) NULL,
    [Over Dimension Fee] VARCHAR(255) NULL,
    [Over Max Size Fee] VARCHAR(255) NULL,
    [Over-weight Fee] VARCHAR(255) NULL,
    [Fuel Surcharge] VARCHAR(255) NULL,
    [Peak Season Surcharge] VARCHAR(255) NULL,
    [Delivery Area Surcharge] VARCHAR(255) NULL,
    [Delivery Area Surcharge Extend] VARCHAR(255) NULL,
    [Truck Fee] VARCHAR(255) NULL,
    [Relabel Fee] VARCHAR(255) NULL,
    [Miscellaneous Fee] VARCHAR(255) NULL,
    [Credit] VARCHAR(255) NULL,
    [Approved Claim] VARCHAR(255) NULL,
    [Credit Card Surcharge] VARCHAR(255) NULL,
    [Total Billed Amount] VARCHAR(255) NULL,
    [Induction Facility] VARCHAR(255) NULL,
    [Induction Facility ZipCode] VARCHAR(255) NULL,
    [Consignee Address] VARCHAR(255) NULL,
    [Destination Facility] VARCHAR(255) NULL,
    [Destination ZipCode] VARCHAR(255) NULL,
    [Channel Code] VARCHAR(255) NULL,
    [Zone] VARCHAR(255) NULL,
    [Manifest Billing] VARCHAR(255) NULL,
    [Billable Weight] VARCHAR(255) NULL,
    [Billable Weight UOM] VARCHAR(255) NULL,
    [Scaled Weight] VARCHAR(255) NULL,
    [Scaled Weight UOM] VARCHAR(255) NULL,
    [Manifested Weight(lb/oz)] VARCHAR(255) NULL,
    [Manifested Weight UOM(lb/oz)] VARCHAR(255) NULL,
    [Manifested Weight] VARCHAR(255) NULL,
    [Manifested Weight UOM] VARCHAR(255) NULL,
    [IS Rated As DIM Weight] VARCHAR(255) NULL,
    [DIM Factor] VARCHAR(255) NULL,
    [Dim Weight(lbs)] VARCHAR(255) NULL,
    [Dim Weight UOM(lbs)] VARCHAR(255) NULL,
    [Dim Weight] VARCHAR(255) NULL,
    [Dim Weight UOM] VARCHAR(255) NULL,
    [Package Length] VARCHAR(255) NULL,
    [Package Width] VARCHAR(255) NULL,
    [Package Height] VARCHAR(255) NULL,
    [Package DIM UOM] VARCHAR(255) NULL,
    [Shipping Order Created Time] VARCHAR(255) NULL,
    [Induction Time] VARCHAR(255) NULL,
    [Billable Time] VARCHAR(255) NULL,
    [GUID Created Time] VARCHAR(255) NULL,
    [Shipped Time] VARCHAR(255) NULL,
    [Delivered Time] VARCHAR(255) NULL,
    [Service Type] VARCHAR(255) NULL
);
