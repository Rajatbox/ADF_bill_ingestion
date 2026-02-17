/*
================================================================================
Database Schema Definition
================================================================================
Note: Database name is omitted - will be parameterized via ADF Linked Service
      per environment (DEV/UAT/PROD). Use only schema.table format.
================================================================================
*/

/*
================================================================================
Delta Tables
================================================================================
*/

CREATE TABLE billing.delta_fedex_bill ( -- rename to delta_fedex_bill
	[Consolidated Account Number] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Bill to Account Number] int NULL,
	[Invoice Date] int NULL,
	[Invoice Number] int NULL,
	[Store ID] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Amount Due] decimal(18,2) NULL,
	[Current Balance] decimal(18,2) NULL,
	Payor varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Ground Tracking ID Prefix] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Express or Ground Tracking ID] varchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Transportation Charge Amount] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Net Charge Amount] decimal(18,2) NULL,
	[Service Type] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Ground Service] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipment Date] int NULL,
	[POD Delivery Date] int NULL,
	[POD Delivery Time] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[POD Service Area Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[POD Signature Description] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Actual Weight Amount] decimal(18,2) NULL,
	[Actual Weight Units] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Rated Weight Amount] decimal(18,2) NULL,
	[Rated Weight Units] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Number of Pieces] int NULL,
	[Bundle Number] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Meter Number] int NULL,
	TDMasterTrackingID bigint NULL,
	[Service Packaging] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Dim Length] int NULL,
	[Dim Width] int NULL,
	[Dim Height] int NULL,
	[Dim Divisor] int NULL,
	[Dim Unit] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient Name] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient Company] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient Address Line 1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient Address Line 2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient City] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient State] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient Zip Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Recipient Country/Territory] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper Company] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper Name] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper Address Line 1] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper Address Line 2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper City] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper State] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipper Zip Code] int NULL,
	[Shipper Country/Territory] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Customer Reference] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Ref#2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Ref#3/PO Number] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Department Reference Description] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Updated Customer Reference] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Updated Ref#2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Updated Ref#3/PO Number] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Updated Department Reference Description] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[RMA#] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Recipient Address Line 1] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Recipient Address Line 2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Recipient City] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Recipient State] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Original Recipient Zip Code] int NULL,
	[Original Recipient Country/Territory] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Zone Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Cost Allocation] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Alternate Address Line 1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Alternate Address Line 2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Alternate City] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Alternate State Province] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Alternate Zip Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Alternate Country/Territory Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CrossRefTrackingID Prefix] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CrossRefTrackingID varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Entry Date] int NULL,
	[Entry Number] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Customs Value] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Customs Value Currency Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Declared Value] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Declared Value Currency Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Description] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Country/Territory Code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Description_1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Country/Territory Code_1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Description_2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Country/Territory Code_2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Description_3] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Commodity Country/Territory Code_3] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Currency Conversion Date] int NULL,
	[Currency Conversion Rate] decimal(18,2) NULL,
	[Multiweight Number] int NULL,
	[Multiweight Total Multiweight Units] int NULL,
	[Multiweight Total Multiweight Weight] decimal(18,2) NULL,
	[Multiweight Total Shipment Charge Amount] decimal(18,2) NULL,
	[Multiweight Total Shipment Weight] decimal(18,2) NULL,
	[Ground Tracking ID Address Correction Discount Charge Amount] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Ground Tracking ID Address Correction Gross Charge Amount] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Rated Method] varchar(50) NULL,
	[Sort Hub] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Estimated Weight] decimal(18,2) NULL,
	[Estimated Weight Unit] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Postal Class] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Process Category] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Package Size] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Delivery Confirmation] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tendered Date] int NULL,
	[MPS Package ID] varchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_3] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_3] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_4] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_4] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_5] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_5] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_6] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_6] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_7] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_7] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_8] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_8] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_9] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_9] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_10] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_10] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_11] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_11] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_12] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_12] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_13] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_13] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_14] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_14] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_15] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_15] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_16] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_16] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_17] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_17] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_18] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_18] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_19] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_19] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_20] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_20] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_21] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_21] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_22] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_22] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_23] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_23] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_24] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_24] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_25] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_25] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_26] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_26] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_27] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_27] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_28] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_28] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_29] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_29] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_30] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_30] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_31] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_31] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_32] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_32] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_33] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_33] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_34] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_34] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_35] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_35] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_36] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_36] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_37] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_37] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_38] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_38] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_39] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_39] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_40] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_40] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_41] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_41] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_42] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_42] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_43] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_43] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_44] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_44] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_45] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_45] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_46] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_46] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_47] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_47] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_48] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_48] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_49] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_49] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Description_50] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_50] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Shipment Notes] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

-- USPS EASYPOST DELTA TABLE
CREATE TABLE billing.delta_usps_easypost_bill (
	[created_at] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[id] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[tracking_code] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[status] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[from_city] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[from_state] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[from_zip] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_name] varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_company] varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_phone] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_email] varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_street1] varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_street2] varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_city] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_state] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_zip] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[to_country] varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[length] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[width] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[height] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[weight] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[predefined_package] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[postage_label_created_at] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[service] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[carrier] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[rate] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[refund_status] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[label_fee] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[postage_fee] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[insurance_fee] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[carbon_offset_fee] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[usps_zone] varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[carrier_account_id] varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

-- UPS DELTA TABLE
CREATE TABLE billing.delta_ups_bill (
    [Version] VARCHAR(50) NULL,
    [Recipient Number] VARCHAR(50) NULL,
    [Account Number] VARCHAR(50) NULL,
    [Account Country] VARCHAR(50) NULL,
    [Invoice Date] VARCHAR(50) NULL,
    [Invoice Number] VARCHAR(50) NULL,
    [Invoice Type Code] VARCHAR(50) NULL,
    [Invoice Type Detail Code] VARCHAR(50) NULL,
    [Account Tax ID] VARCHAR(50) NULL,
    [Invoice Currency Code] VARCHAR(50) NULL,
    [Invoice Amount] VARCHAR(50) NULL,
    [Transaction Date] VARCHAR(50) NULL,
    [Pickup Record Number] VARCHAR(50) NULL,
    [Lead Shipment Number] VARCHAR(50) NULL,
    [World Ease Number] VARCHAR(50) NULL,
    [Shipment Reference Number 1] VARCHAR(500) NULL,
    [Shipment Reference Number 2] VARCHAR(500) NULL,
    [Bill Option Code] VARCHAR(50) NULL,
    [Package Quantity] VARCHAR(50) NULL,
    [Oversize Quantity] VARCHAR(50) NULL,
    [Tracking Number] VARCHAR(255) NULL,
    [Package Reference Number 1] VARCHAR(500) NULL,
    [Package Reference Number 2] VARCHAR(500) NULL,
    [Package Reference Number 3] VARCHAR(500) NULL,
    [Package Reference Number 4] VARCHAR(500) NULL,
    [Package Reference Number 5] VARCHAR(500) NULL,
    [Entered Weight] VARCHAR(50) NULL,
    [Entered Weight Unit of Measure] VARCHAR(50) NULL,
    [Billed Weight] VARCHAR(50) NULL,
    [Billed Weight Unit of Measure] VARCHAR(50) NULL,
    [Container Type] VARCHAR(50) NULL,
    [Billed Weight Type] VARCHAR(50) NULL,
    [Package Dimensions] VARCHAR(100) NULL,
    [Zone] VARCHAR(50) NULL,
    [Charge Category Code] VARCHAR(50) NULL,
    [Charge Category Detail Code] VARCHAR(50) NULL,
    [Charge Source] VARCHAR(50) NULL,
    [Type Code 1] VARCHAR(50) NULL,
    [Type Detail Code 1] VARCHAR(50) NULL,
    [Type Detail Value 1] VARCHAR(50) NULL,
    [Type Code 2] VARCHAR(50) NULL,
    [Type Detail Code 2] VARCHAR(50) NULL,
    [Type Detail Value 2] VARCHAR(50) NULL,
    [Charge Classification Code] VARCHAR(50) NULL,
    [Charge Description Code] VARCHAR(50) NULL,
    [Charge Description] VARCHAR(255) NULL,
    [Charged Unit Quantity] VARCHAR(50) NULL,
    [Basis Currency Code] VARCHAR(50) NULL,
    [Basis Value] VARCHAR(50) NULL,
    [Tax Indicator] VARCHAR(50) NULL,
    [Transaction Currency Code] VARCHAR(50) NULL,
    [Incentive Amount] VARCHAR(50) NULL,
    [Net Amount] VARCHAR(50) NULL,
    [Miscellaneous Currency Code] VARCHAR(50) NULL,
    [Miscellaneous Incentive Amount] VARCHAR(50) NULL,
    [Miscellaneous Net Amount] VARCHAR(50) NULL,
    [Alternate Invoicing Currency Code] VARCHAR(50) NULL,
    [Alternate Invoice Amount] VARCHAR(50) NULL,
    [Invoice Exchange Rate] VARCHAR(50) NULL,
    [Tax Variance Amount] VARCHAR(50) NULL,
    [Currency Variance Amount] VARCHAR(50) NULL,
    [Invoice Level Charge] VARCHAR(50) NULL,
    [Invoice Due Date] VARCHAR(50) NULL,
    [Alternate Invoice Number] VARCHAR(50) NULL,
    [Store Number] VARCHAR(50) NULL,
    [Customer Reference Number] VARCHAR(500) NULL,
    [Sender Name] VARCHAR(255) NULL,
    [Sender Company Name] VARCHAR(255) NULL,
    [Sender Address Line 1] VARCHAR(255) NULL,
    [Sender Address Line 2] VARCHAR(255) NULL,
    [Sender City] VARCHAR(100) NULL,
    [Sender State] VARCHAR(50) NULL,
    [Sender Postal] VARCHAR(100) NULL,
    [Sender Country] VARCHAR(50) NULL,
    [Receiver Name] VARCHAR(255) NULL,
    [Receiver Company Name] VARCHAR(255) NULL,
    [Receiver Address Line 1] VARCHAR(255) NULL,
    [Receiver Address Line 2] VARCHAR(255) NULL,
    [Receiver City] VARCHAR(100) NULL,
    [Receiver State] VARCHAR(50) NULL,
    [Receiver Postal] VARCHAR(100) NULL,
    [Receiver Country] VARCHAR(50) NULL,
    [Third Party Name] VARCHAR(255) NULL,
    [Third Party Company Name] VARCHAR(255) NULL,
    [Third Party Address Line 1] VARCHAR(255) NULL,
    [Third Party Address Line 2] VARCHAR(255) NULL,
    [Third Party City] VARCHAR(100) NULL,
    [Third Party State] VARCHAR(50) NULL,
    [Third Party Postal] VARCHAR(100) NULL,
    [Third Party Country] VARCHAR(50) NULL,
    [Sold To Name] VARCHAR(255) NULL,
    [Sold To Company Name] VARCHAR(255) NULL,
    [Sold To Address Line 1] VARCHAR(255) NULL,
    [Sold To Address Line 2] VARCHAR(255) NULL,
    [Sold To City] VARCHAR(100) NULL,
    [Sold To State] VARCHAR(50) NULL,
    [Sold To Postal] VARCHAR(100) NULL,
    [Sold To Country] VARCHAR(50) NULL,
    [Miscellaneous Address Qual 1] VARCHAR(50) NULL,
    [Miscellaneous Address 1 Name] VARCHAR(255) NULL,
    [Miscellaneous Address 1 Company Name] VARCHAR(255) NULL,
    [Miscellaneous Address 1 Address Line 1] VARCHAR(255) NULL,
    [Miscellaneous Address 1 Address Line 2] VARCHAR(255) NULL,
    [Miscellaneous Address 1 City] VARCHAR(100) NULL,
    [Miscellaneous Address 1 State] VARCHAR(50) NULL,
    [Miscellaneous Address 1 Postal] VARCHAR(100) NULL,
    [Miscellaneous Address 1 Country] VARCHAR(50) NULL,
    [Miscellaneous Address Qual 2] VARCHAR(50) NULL,
    [Miscellaneous Address 2 Name] VARCHAR(255) NULL,
    [Miscellaneous Address 2 Company Name] VARCHAR(255) NULL,
    [Miscellaneous Address 2 Address Line 1] VARCHAR(255) NULL,
    [Miscellaneous Address 2 Address Line 2] VARCHAR(255) NULL,
    [Miscellaneous Address 2 City] VARCHAR(100) NULL,
    [Miscellaneous Address 2 State] VARCHAR(50) NULL,
    [Miscellaneous Address 2 Postal] VARCHAR(100) NULL,
    [Miscellaneous Address 2 Country] VARCHAR(50) NULL,
    [Shipment Date] VARCHAR(50) NULL,
    [Shipment Export Date] VARCHAR(50) NULL,
    [Shipment Import Date] VARCHAR(50) NULL,
    [Entry Date] VARCHAR(50) NULL,
    [Direct Shipment Date] VARCHAR(50) NULL,
    [Shipment Delivery Date] VARCHAR(50) NULL,
    [Shipment Release Date] VARCHAR(50) NULL,
    [Cycle Date] VARCHAR(50) NULL,
    [EFT Date] VARCHAR(50) NULL,
    [Validation Date] VARCHAR(50) NULL,
    [Entry Port] VARCHAR(100) NULL,
    [Entry Number] VARCHAR(100) NULL,
    [Export Place] VARCHAR(100) NULL,
    [Shipment Value Amount] VARCHAR(50) NULL,
    [Shipment Description] VARCHAR(500) NULL,
    [Entered Currency Code] VARCHAR(50) NULL,
    [Customs Number] VARCHAR(100) NULL,
    [Exchange Rate] VARCHAR(50) NULL,
    [Master Air Waybill Number] VARCHAR(100) NULL,
    [EPU] VARCHAR(100) NULL,
    [Entry Type] VARCHAR(50) NULL,
    [CPC Code] VARCHAR(50) NULL,
    [Line Item Number] VARCHAR(50) NULL,
    [Goods Description] VARCHAR(500) NULL,
    [Entered Value] VARCHAR(50) NULL,
    [Duty Amount] VARCHAR(50) NULL,
    [Weight] VARCHAR(50) NULL,
    [Unit of Measure] VARCHAR(50) NULL,
    [Item Quantity] VARCHAR(50) NULL,
    [Item Quantity Unit of Measure] VARCHAR(50) NULL,
    [Import Tax ID] VARCHAR(100) NULL,
    [Declaration Number] VARCHAR(100) NULL,
    [Carrier Name] VARCHAR(255) NULL,
    [CCCD Number] VARCHAR(100) NULL,
    [Cycle Number] VARCHAR(100) NULL,
    [Foreign Trade Reference Number] VARCHAR(100) NULL,
    [Job Number] VARCHAR(100) NULL,
    [Transport Mode] VARCHAR(50) NULL,
    [Tax Type] VARCHAR(50) NULL,
    [Tariff Code] VARCHAR(100) NULL,
    [Tariff Rate] VARCHAR(50) NULL,
    [Tariff Treatment Number] VARCHAR(100) NULL,
    [Contact Name] VARCHAR(255) NULL,
    [Class Number] VARCHAR(50) NULL,
    [Document Type] VARCHAR(50) NULL,
    [Office Number] VARCHAR(100) NULL,
    [Document Number] VARCHAR(100) NULL,
    [Duty Value] VARCHAR(50) NULL,
    [Total Value for Duty] VARCHAR(50) NULL,
    [Excise Tax Amount] VARCHAR(50) NULL,
    [Excise Tax Rate] VARCHAR(50) NULL,
    [GST Amount] VARCHAR(50) NULL,
    [GST Rate] VARCHAR(50) NULL,
    [Order In Council] VARCHAR(100) NULL,
    [Origin Country] VARCHAR(50) NULL,
    [SIMA Access] VARCHAR(50) NULL,
    [Tax Value] VARCHAR(50) NULL,
    [Total Customs Amount] VARCHAR(50) NULL,
    [Miscellaneous Line 1] VARCHAR(500) NULL,
    [Miscellaneous Line 2] VARCHAR(500) NULL,
    [Miscellaneous Line 3] VARCHAR(500) NULL,
    [Miscellaneous Line 4] VARCHAR(500) NULL,
    [Miscellaneous Line 5] VARCHAR(500) NULL,
    [Payor Role Code] VARCHAR(50) NULL,
    [Miscellaneous Line 7] VARCHAR(500) NULL,
    [Miscellaneous Line 8] VARCHAR(500) NULL,
    [Miscellaneous Line 9] VARCHAR(500) NULL,
    [Miscellaneous Line 10] VARCHAR(500) NULL,
    [Miscellaneous Line 11] VARCHAR(500) NULL,
    [Duty Rate] VARCHAR(50) NULL,
    [VAT Basis Amount] VARCHAR(50) NULL,
    [VAT Amount] VARCHAR(50) NULL,
    [VAT Rate] VARCHAR(50) NULL,
    [Other Basis Amount] VARCHAR(50) NULL,
    [Other Amount] VARCHAR(50) NULL,
    [Other Rate] VARCHAR(50) NULL,
    [Other Customs Number Indicator] VARCHAR(50) NULL,
    [Other Customs Number] VARCHAR(100) NULL,
    [Customs Office Name] VARCHAR(255) NULL,
    [Package Dimension Unit Of Measure] VARCHAR(50) NULL,
    [Original Shipment Package Quantity] VARCHAR(50) NULL,
    [Place Holder 24] VARCHAR(50) NULL,
    [Place Holder 25] VARCHAR(50) NULL,
    [Place Holder 26] VARCHAR(50) NULL,
    [Place Holder 27] VARCHAR(50) NULL,
    [Place Holder 28] VARCHAR(50) NULL,
    [Place Holder 29] VARCHAR(50) NULL,
    [Place Holder 30] VARCHAR(50) NULL,
    [Place Holder 31] VARCHAR(50) NULL,
    [BOL # 1] VARCHAR(100) NULL,
    [BOL # 2] VARCHAR(100) NULL,
    [BOL # 3] VARCHAR(100) NULL,
    [BOL # 4] VARCHAR(100) NULL,
    [BOL # 5] VARCHAR(100) NULL,
    [PO # 1] VARCHAR(100) NULL,
    [PO # 2] VARCHAR(100) NULL,
    [PO # 3] VARCHAR(100) NULL,
    [PO # 4] VARCHAR(100) NULL,
    [PO # 5] VARCHAR(100) NULL,
    [PO # 6] VARCHAR(100) NULL,
    [PO # 7] VARCHAR(100) NULL,
    [PO # 8] VARCHAR(100) NULL,
    [PO # 9] VARCHAR(100) NULL,
    [PO # 10] VARCHAR(100) NULL,
    [NMFC] VARCHAR(100) NULL,
    [Detail Class] VARCHAR(50) NULL,
    [Freight Sequence Number] VARCHAR(50) NULL,
    [Declared Freight Class] VARCHAR(50) NULL,
    [Place Holder 34] VARCHAR(50) NULL,
    [Place Holder 35] VARCHAR(50) NULL,
    [Place Holder 36] VARCHAR(50) NULL,
    [Place Holder 37] VARCHAR(50) NULL,
    [Place Holder 38] VARCHAR(50) NULL,
    [Place Holder 39] VARCHAR(50) NULL,
    [Place Holder 40] VARCHAR(50) NULL,
    [Place Holder 41] VARCHAR(50) NULL,
    [Place Holder 42] VARCHAR(50) NULL,
    [Place Holder 43] VARCHAR(50) NULL,
    [Place Holder 44] VARCHAR(50) NULL,
    [Place Holder 45] VARCHAR(50) NULL,
    [Place Holder 46] VARCHAR(50) NULL,
    [Place Holder 47] VARCHAR(50) NULL,
    [Place Holder 48] VARCHAR(50) NULL,
    [Place Holder 49] VARCHAR(50) NULL,
    [Place Holder 50] VARCHAR(50) NULL,
    [Place Holder 51] VARCHAR(50) NULL,
    [Place Holder 52] VARCHAR(50) NULL,
    [Place Holder 53] VARCHAR(50) NULL
);

-- DHL DELTA TABLE
-- Headerless CSV: 79 columns. Names follow additional_reference.md exactly.
-- Named columns use the documented name; unnamed columns use Prop_N (N = column number).
-- ADF Prop (0-indexed) shown in comments for ADF column mapping configuration.
CREATE TABLE billing.delta_dhl_bill (
	Prop_1 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 1  (ADF: Prop_0)
	account_number varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                          -- Col 2  (ADF: Prop_1):  Account Number
	Prop_3 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 3  (ADF: Prop_2)
	Prop_4 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 4  (ADF: Prop_3)
	Prop_5 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 5  (ADF: Prop_4)
	Prop_6 varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 6  (ADF: Prop_5)
	Prop_7 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 7  (ADF: Prop_6)
	Prop_8 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 8  (ADF: Prop_7)
	shipping_date varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                           -- Col 9  (ADF: Prop_8):  Shipping Date
	Prop_10 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 10 (ADF: Prop_9)
	Prop_11 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 11 (ADF: Prop_10)
	international_tracking_number varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,          -- Col 12 (ADF: Prop_11): Tracking Number (International Shipments)
	domestic_tracking_number varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,               -- Col 13 (ADF: Prop_12): unique_id (Domestic Shipments)
	Prop_14 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 14 (ADF: Prop_13)
	recipient_address_line_1 varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,               -- Col 15 (ADF: Prop_14): Recipient Address Line 1
	recipient_address_line_2 varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,               -- Col 16 (ADF: Prop_15): Recipient Address Line 2
	recipient_city varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                         -- Col 17 (ADF: Prop_16): Recipient City
	recipient_state_province varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                -- Col 18 (ADF: Prop_17): Recipient State/Province
	recipient_zip_postal_code varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,               -- Col 19 (ADF: Prop_18): Recipient Zip/Postal Code
	recipient_country varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                       -- Col 20 (ADF: Prop_19): Recipient Country
	Prop_21 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 21 (ADF: Prop_20)
	shipping_method varchar(350) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                        -- Col 22 (ADF: Prop_21): Shipping Method
	shipped_weight varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                          -- Col 23 (ADF: Prop_22): Shipped Weight
	shipped_weight_unit_of_measure varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,          -- Col 24 (ADF: Prop_23): Shipped Weight Unit of Measure
	billed_weight varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                           -- Col 25 (ADF: Prop_24): Billed Weight
	billed_weight_unit_of_measure varchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,           -- Col 26 (ADF: Prop_25): Billed Weight Unit of Measure
	Prop_27 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 27 (ADF: Prop_26)
	Prop_28 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 28 (ADF: Prop_27)
	[zone] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                  -- Col 29 (ADF: Prop_28): Zone
	transportation_cost varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                     -- Col 30 (ADF: Prop_29): Transportation Cost
	Prop_31 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 31 (ADF: Prop_30)
	Prop_32 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 32 (ADF: Prop_31)
	Prop_33 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 33 (ADF: Prop_32)
	Prop_34 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 34 (ADF: Prop_33)
	Prop_35 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 35 (ADF: Prop_34)
	Prop_36 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 36 (ADF: Prop_35)
	Prop_37 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 37 (ADF: Prop_36)
	Prop_38 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 38 (ADF: Prop_37)
	Prop_39 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 39 (ADF: Prop_38)
	Prop_40 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 40 (ADF: Prop_39)
	Prop_41 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 41 (ADF: Prop_40)
	Prop_42 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 42 (ADF: Prop_41)
	Prop_43 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 43 (ADF: Prop_42)
	Prop_44 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 44 (ADF: Prop_43)
	Prop_45 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 45 (ADF: Prop_44)
	Prop_46 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 46 (ADF: Prop_45)
	Prop_47 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 47 (ADF: Prop_46)
	non_qualified_dimensional_charges varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,       -- Col 48 (ADF: Prop_47): Non-Qualified Dimensional Charges
	Prop_49 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 49 (ADF: Prop_48)
	Prop_50 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 50 (ADF: Prop_49)
	Prop_51 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 51 (ADF: Prop_50)
	Prop_52 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 52 (ADF: Prop_51)
	Prop_53 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 53 (ADF: Prop_52)
	Prop_54 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 54 (ADF: Prop_53)
	Prop_55 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 55 (ADF: Prop_54)
	Prop_56 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 56 (ADF: Prop_55)
	Prop_57 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 57 (ADF: Prop_56)
	Prop_58 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 58 (ADF: Prop_57)
	Prop_59 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 59 (ADF: Prop_58)
	Prop_60 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 60 (ADF: Prop_59)
	Prop_61 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 61 (ADF: Prop_60)
	Prop_62 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 62 (ADF: Prop_61)
	Prop_63 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 63 (ADF: Prop_62)
	Prop_64 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 64 (ADF: Prop_63)
	fuel_surcharge_amount varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                   -- Col 65 (ADF: Prop_64): Fuel Surcharge Amount
	Prop_66 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 66 (ADF: Prop_65)
	overlabel_tracking_number varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,              -- Col 67 (ADF: Prop_66): Overlabel Tracking Number
	Prop_68 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 68 (ADF: Prop_67)
	Prop_69 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 69 (ADF: Prop_68)
	Prop_70 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 70 (ADF: Prop_69)
	Prop_71 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 71 (ADF: Prop_70)
	Prop_72 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 72 (ADF: Prop_71)
	Prop_73 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 73 (ADF: Prop_72)
	Prop_74 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 74 (ADF: Prop_73)
	Prop_75 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 75 (ADF: Prop_74)
	Prop_76 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 76 (ADF: Prop_75)
	Prop_77 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 77 (ADF: Prop_76)
	Prop_78 varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                 -- Col 78 (ADF: Prop_77)
	delivery_area_surcharge_amount varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,          -- Col 79 (ADF: Prop_78): Delivery Area Surcharge Amount
	-- ADF-appended columns from HDR row:
	invoice_number varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	invoice_date varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

--ELITEWORKS DELTA TABLE
CREATE TABLE billing.delta_eliteworks_bill (
    time_utc VARCHAR(50) NULL,
    shipment_id VARCHAR(255) NULL,
    user_account VARCHAR(255) NULL,
    tracking_number VARCHAR(255) NULL,
    status VARCHAR(50) NULL,
    carrier VARCHAR(50) NULL,
    service VARCHAR(255) NULL,
    reference VARCHAR(255) NULL,
    shipment_weight_oz VARCHAR(50) NULL,
    shipment_dryice_weight_oz VARCHAR(50) NULL,
    package_type VARCHAR(50) NULL,
    package_length_in VARCHAR(50) NULL,
    package_width_in VARCHAR(50) NULL,
    package_height_in VARCHAR(50) NULL,
    from_name VARCHAR(255) NULL,
    from_company VARCHAR(255) NULL,
    from_street VARCHAR(255) NULL,
    from_apt_suite VARCHAR(50) NULL,
    from_city VARCHAR(100) NULL,
    from_state VARCHAR(50) NULL,
    from_postal VARCHAR(50) NULL,
    from_country VARCHAR(10) NULL,
    to_name VARCHAR(255) NULL,
    to_company VARCHAR(255) NULL,
    to_street VARCHAR(255) NULL,
    to_apt_suite VARCHAR(50) NULL,
    to_city VARCHAR(100) NULL,
    to_state VARCHAR(50) NULL,
    to_postal VARCHAR(50) NULL,
    to_country VARCHAR(10) NULL,
    first_scan VARCHAR(50) NULL,
    delivered VARCHAR(50) NULL,
    delivered_days VARCHAR(50) NULL,
    delivered_business_days VARCHAR(50) NULL,
    zone VARCHAR(50) NULL,
    charged VARCHAR(50) NULL,
    store_markup VARCHAR(50) NULL,
    platform_charged_with_corrections VARCHAR(50) NULL,
    commercial VARCHAR(50) NULL,
    order_reference VARCHAR(255) NULL,
    order_date VARCHAR(50) NULL
);


/*
================================================================================
Normalized Carrier Tables
================================================================================
*/

--FEDEX BILL TABLE

CREATE TABLE billing.fedex_bill (
	id int IDENTITY(1,1) NOT NULL,
	invoice_number nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	invoice_date date NOT NULL,
	service_type nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	shipment_date date NULL,
	express_or_ground_tracking_id nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	msp_tracking_id nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	zone_code nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	net_charge_amount decimal(18,2) NULL,
	rated_weight_amount decimal(18,2) NULL,
	rated_weight_units nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	dim_length decimal(18,2) NULL,
	dim_width decimal(18,2) NULL,
	dim_height decimal(18,2) NULL,
	dim_unit nvarchar(9) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,
	carrier_bill_id int NULL,
	shipper_zip_code nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Transportation Charge Amount] decimal(18,2) NULL,
	[Tracking ID Charge Description] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount] decimal(18,2) NULL,
	[Tracking ID Charge Description_1] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_1] decimal(18,2) NULL,
	[Tracking ID Charge Description_2] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_2] decimal(18,2) NULL,
	[Tracking ID Charge Description_3] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_3] decimal(18,2) NULL,
	[Tracking ID Charge Description_4] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_4] decimal(18,2) NULL,
	[Tracking ID Charge Description_5] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_5] decimal(18,2) NULL,
	[Tracking ID Charge Description_6] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_6] decimal(18,2) NULL,
	[Tracking ID Charge Description_7] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_7] decimal(18,2) NULL,
	[Tracking ID Charge Description_8] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_8] decimal(18,2) NULL,
	[Tracking ID Charge Description_9] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_9] decimal(18,2) NULL,
	[Tracking ID Charge Description_10] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_10] decimal(18,2) NULL,
	[Tracking ID Charge Description_11] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_11] decimal(18,2) NULL,
	[Tracking ID Charge Description_12] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_12] decimal(18,2) NULL,
	[Tracking ID Charge Description_13] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_13] decimal(18,2) NULL,
	[Tracking ID Charge Description_14] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_14] decimal(18,2) NULL,
	[Tracking ID Charge Description_15] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_15] decimal(18,2) NULL,
	[Tracking ID Charge Description_16] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_16] decimal(18,2) NULL,
	[Tracking ID Charge Description_17] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_17] decimal(18,2) NULL,
	[Tracking ID Charge Description_18] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_18] decimal(18,2) NULL,
	[Tracking ID Charge Description_19] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_19] decimal(18,2) NULL,
	[Tracking ID Charge Description_20] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_20] decimal(18,2) NULL,
	[Tracking ID Charge Description_21] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_21] decimal(18,2) NULL,
	[Tracking ID Charge Description_22] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_22] decimal(18,2) NULL,
	[Tracking ID Charge Description_23] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_23] decimal(18,2) NULL,
	[Tracking ID Charge Description_24] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_24] decimal(18,2) NULL,
	[Tracking ID Charge Description_25] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_25] decimal(18,2) NULL,
	[Tracking ID Charge Description_26] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_26] decimal(18,2) NULL,
	[Tracking ID Charge Description_27] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_27] decimal(18,2) NULL,
	[Tracking ID Charge Description_28] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_28] decimal(18,2) NULL,
	[Tracking ID Charge Description_29] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_29] decimal(18,2) NULL,
	[Tracking ID Charge Description_30] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_30] decimal(18,2) NULL,
	[Tracking ID Charge Description_31] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_31] decimal(18,2) NULL,
	[Tracking ID Charge Description_32] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_32] decimal(18,2) NULL,
	[Tracking ID Charge Description_33] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_33] decimal(18,2) NULL,
	[Tracking ID Charge Description_34] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_34] decimal(18,2) NULL,
	[Tracking ID Charge Description_35] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_35] decimal(18,2) NULL,
	[Tracking ID Charge Description_36] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_36] decimal(18,2) NULL,
	[Tracking ID Charge Description_37] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_37] decimal(18,2) NULL,
	[Tracking ID Charge Description_38] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_38] decimal(18,2) NULL,
	[Tracking ID Charge Description_39] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_39] decimal(18,2) NULL,
	[Tracking ID Charge Description_40] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_40] decimal(18,2) NULL,
	[Tracking ID Charge Description_41] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_41] decimal(18,2) NULL,
	[Tracking ID Charge Description_42] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_42] decimal(18,2) NULL,
	[Tracking ID Charge Description_43] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_43] decimal(18,2) NULL,
	[Tracking ID Charge Description_44] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_44] decimal(18,2) NULL,
	[Tracking ID Charge Description_45] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_45] decimal(18,2) NULL,
	[Tracking ID Charge Description_46] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_46] decimal(18,2) NULL,
	[Tracking ID Charge Description_47] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_47] decimal(18,2) NULL,
	[Tracking ID Charge Description_48] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_48] decimal(18,2) NULL,
	[Tracking ID Charge Description_49] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_49] decimal(18,2) NULL,
	[Tracking ID Charge Description_50] varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Tracking ID Charge Amount_50] decimal(18,2) NULL,
	
	CONSTRAINT PK_fedex_carrier_bill_id PRIMARY KEY (id),
	CONSTRAINT FK_fedex_bill_carrier_bill FOREIGN KEY (carrier_bill_id) 
		REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_fedex_bill_carrier_bill_id
ON billing.fedex_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_fedex_bill_created_date
ON billing.fedex_bill (created_date);

-- Composite index for tracking number lookups (used in mapping queries)
CREATE NONCLUSTERED INDEX IX_fedex_bill_tracking_number_invoice
ON billing.fedex_bill (express_or_ground_tracking_id, invoice_number, invoice_date);

-- USPS EASYPOST BILL TABLE
CREATE TABLE billing.usps_easy_post_bill (
	id bigint IDENTITY(1,1) NOT NULL,
	tracking_code varchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	invoice_number varchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	carrier_bill_id int NULL,
	weight decimal(18,2) NULL,
	rate decimal(18,2) NULL,
	label_fee decimal(18,2) NULL,
	postage_fee decimal(18,2) NULL,
	usps_zone tinyint NULL,
	from_zip char(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[length] decimal(18,2) NULL,
	width decimal(18,2) NULL,
	height decimal(18,2) NULL,
	postage_label_created_at datetime2(0) NULL,
	insurance_fee decimal(18,2) NULL,
	carbon_offset_fee decimal(18,2) NULL,
	bill_date datetime2(0) NOT NULL,
	service varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	created_at datetime2(0) DEFAULT sysdatetime() NOT NULL,
	CONSTRAINT PK__usps_eas__3213E83F62CA7274 PRIMARY KEY (id)
);

--UPS BILL TABLE

CREATE TABLE billing.ups_bill (
	id int IDENTITY(1,1) NOT NULL,
	invoice_number nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	invoice_date date NOT NULL,
	charge_description nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	charge_classification_code nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	charge_category_code nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	charge_category_detail_code nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	transaction_date datetime2 NULL,
	tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[zone] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	net_amount decimal(18,2) NOT NULL,
	billed_weight decimal(18,2) NULL,
	billed_weight_unit nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	dim_length decimal(18,2) NULL,
	dim_width decimal(18,2) NULL,
	dim_height decimal(18,2) NULL,
	dim_unit nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,
	carrier_bill_id int NULL,
	sender_postal nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	
	CONSTRAINT PK_ups_carrier_bill_id PRIMARY KEY (id),
	CONSTRAINT FK_ups_bill_carrier_bill FOREIGN KEY (carrier_bill_id) 
		REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_ups_bill_carrier_bill_id
ON billing.ups_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_ups_bill_created_date
ON billing.ups_bill (created_date);

-- Composite index for tracking number lookups (used in mapping queries)
CREATE NONCLUSTERED INDEX IX_ups_bill_tracking_number_invoice
ON billing.ups_bill (tracking_number, invoice_number, invoice_date);



-- DHL BILL TABLE (Normalized carrier bill line items)
-- Column order follows the real bill (CSV) column sequence
CREATE TABLE billing.dhl_bill (
	id int IDENTITY(1,1) NOT NULL,
	carrier_bill_id int NULL,
	invoice_number nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	invoice_date date NOT NULL,
	shipping_date date NULL,                                                                       -- Col 9
	international_tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,         -- Col 12: as-is from CSV
	domestic_tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,              -- Col 13: computed '420' + zip5 + unique_id
	recipient_zip_postal_code nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,             -- Col 19
	recipient_country nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                      -- Col 20
	shipping_method nvarchar(350) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                       -- Col 22
	shipped_weight decimal(18,2) NULL,                                                             -- Col 23
	shipped_weight_unit nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                    -- Col 24
	billed_weight decimal(18,2) NULL,                                                              -- Col 25
	billed_weight_unit nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                     -- Col 26
	[zone] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,                                -- Col 29
	transportation_cost decimal(18,2) NULL,                                                        -- Col 30
	non_qualified_dimensional_charges decimal(18,2) NULL,                                          -- Col 48
	fuel_surcharge_amount decimal(18,2) NULL,                                                      -- Col 65
	delivery_area_surcharge_amount decimal(18,2) NULL,                                             -- Col 79
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,

	CONSTRAINT PK_dhl_bill PRIMARY KEY (id),
	CONSTRAINT FK_dhl_bill_carrier_bill FOREIGN KEY (carrier_bill_id) 
		REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_dhl_bill_carrier_bill_id
ON billing.dhl_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_dhl_bill_created_date
ON billing.dhl_bill (created_date);

-- Composite index for tracking number lookups
CREATE NONCLUSTERED INDEX IX_dhl_bill_tracking
ON billing.dhl_bill (domestic_tracking_number, international_tracking_number, invoice_number);

--ELITEWORKS BILL TABLE (Normalized carrier bill line items)
CREATE TABLE billing.eliteworks_bill (
    id INT IDENTITY(1,1) NOT NULL,
    carrier_bill_id INT NULL,
    invoice_number NVARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    tracking_number NVARCHAR(255) NOT NULL,
    shipment_date DATETIME NULL,
    service_method NVARCHAR(255) NULL,
    zone NVARCHAR(50) NULL,
    charged_amount DECIMAL(18,2) NULL,
    store_markup DECIMAL(18,2) NULL,
    platform_charged DECIMAL(18,2) NULL,
    billed_weight_oz DECIMAL(18,2) NULL,
    package_length_in DECIMAL(18,2) NULL,
    package_width_in DECIMAL(18,2) NULL,
    package_height_in DECIMAL(18,2) NULL,
    from_postal NVARCHAR(50) NULL,
    to_postal NVARCHAR(50) NULL,
    to_city NVARCHAR(100) NULL,
    to_state NVARCHAR(50) NULL,
    to_country NVARCHAR(10) NULL,
    shipment_status NVARCHAR(50) NULL,
    created_date DATETIME2 DEFAULT SYSDATETIME() NOT NULL,
    
    CONSTRAINT PK_eliteworks_bill PRIMARY KEY (id),
    CONSTRAINT FK_eliteworks_bill_carrier_bill FOREIGN KEY (carrier_bill_id)
        REFERENCES billing.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_carrier_bill_id
ON billing.eliteworks_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_created_date
ON billing.eliteworks_bill (created_date);

-- Composite index for tracking number lookups
CREATE NONCLUSTERED INDEX IX_eliteworks_bill_tracking_number_invoice
ON billing.eliteworks_bill (tracking_number, invoice_number, invoice_date);

/*
================================================================================
GOLD LAYER TABLES
================================================================================
*/
CREATE TABLE billing.carrier_bill ( -- rename to carrier_bill
	carrier_bill_id int IDENTITY(1,1) NOT NULL,
	bill_id int NULL,
	carrier_id int NULL,
	bill_date date NULL,
	total_amount decimal(18,2) NULL,
	num_shipments int NULL,
	reconciled_amount decimal(18,2) NULL,
	discrepancy_amount decimal(18,2) NULL,
	bill_number varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	account_number varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK__carrier___A6B346871A6B3E58 PRIMARY KEY (carrier_bill_id)
);

-- Unique constraint: Prevent duplicate invoices per carrier
CREATE UNIQUE NONCLUSTERED INDEX UQ_carrier_bill_number_date_carrier
ON billing.carrier_bill (bill_number, bill_date, carrier_id)
WHERE carrier_id IS NOT NULL AND bill_date IS NOT NULL;

-- Index for carrier-based queries
CREATE NONCLUSTERED INDEX IX_carrier_bill_carrier_id
ON billing.carrier_bill (carrier_id, bill_date);

-----------------------------------------------------------------------------------------------------------------

-- Carrier Ingestion Tracker Table
-- Purpose: Tracks last successful run timestamp per carrier for incremental processing
-- Usage: Updated by Load_to_gold.sql after successful pipeline completion
--        Read by ValidateCarrierInfo.sql to provide @lastrun parameter
CREATE TABLE billing.carrier_ingestion_tracker (
	carrier_name varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	last_run_time datetime2 NOT NULL,
	CONSTRAINT PK_carrier_ingestion_tracker PRIMARY KEY (carrier_name)
);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbo.shipping_method (
	shipping_method_id int NOT NULL,
	carrier_id int NULL,
	method_name nvarchar(510) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	service_level nvarchar(510) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	average_transit_days decimal(18,4) NULL,
	guaranteed_delivery bit NULL,
	is_active bit NULL,
	CONSTRAINT PK__shipping__DCF5023B0C181765 PRIMARY KEY (shipping_method_id)
);

-- Unique constraint: Prevent duplicate shipping methods per carrier
CREATE UNIQUE NONCLUSTERED INDEX UQ_shipping_method_carrier_name
ON dbo.shipping_method (carrier_id, method_name);

-- Index for carrier-based lookups
CREATE NONCLUSTERED INDEX IX_shipping_method_carrier_active
ON dbo.shipping_method (carrier_id, is_active);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbo.charge_types (
	charge_type_id int IDENTITY(1,1) NOT NULL,
	carrier_id int NULL,
	charge_name nvarchar(510) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	freight bit NULL,
	dt bit NULL,
	charge_category_id int NULL,
	markup bit NULL,
	CONSTRAINT PK__charge_t__3BEF5FDF5218DF44 PRIMARY KEY (charge_type_id)
);

-- Unique constraint: Prevent duplicate charge types per carrier
CREATE UNIQUE NONCLUSTERED INDEX UQ_charge_types_carrier_name
ON dbo.charge_types (carrier_id, charge_name);

-- Index for category-based queries
CREATE NONCLUSTERED INDEX IX_charge_types_carrier_category
ON dbo.charge_types (carrier_id, charge_category_id);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE billing.shipment_charges (
	id bigint IDENTITY(1,1) NOT NULL,
	carrier_id int NOT NULL,
	carrier_bill_id int NOT NULL,
	tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	charge_type_id int NOT NULL,
	amount decimal(18,2) NULL,
	shipment_attribute_id bigint NOT NULL,
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,
	CONSTRAINT PK__shipment__charges PRIMARY KEY (id),
	CONSTRAINT FK_shipment_charges_attributes FOREIGN KEY (shipment_attribute_id) 
		REFERENCES billing.shipment_attributes(id)
);

-- Index for FK lookup performance
CREATE NONCLUSTERED INDEX IX_shipment_charges_attribute_id
ON billing.shipment_charges (shipment_attribute_id);

-- Unique constraint: Prevent duplicate charges for same shipment/carrier_bill/charge_type
CREATE UNIQUE NONCLUSTERED INDEX UQ_shipment_charges_bill_tracking_charge
ON billing.shipment_charges (carrier_bill_id, tracking_number, charge_type_id);

-- Index for carrier-based queries
CREATE NONCLUSTERED INDEX IX_shipment_charges_carrier_date
ON billing.shipment_charges (carrier_id, created_date);


-----------------------------------------------------------------------------------------------------------------


CREATE TABLE billing.shipment_attributes (
	id bigint IDENTITY(1,1) NOT NULL,
	carrier_id int NOT NULL,
	shipment_date datetime NULL,
	shipping_method nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	destination_zone varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	billed_weight_oz decimal(18,2) NULL,
	billed_length_in decimal(18,2) NULL,
	billed_width_in decimal(18,2) NULL,
	billed_height_in decimal(18,2) NULL,
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,
	updated_date datetime2 DEFAULT sysdatetime() NOT NULL
	CONSTRAINT PK__shipment__attribute PRIMARY KEY (id)
);

-- Enforce business key uniqueness to prevent duplicate tracking numbers
CREATE UNIQUE NONCLUSTERED INDEX UQ_shipment_attributes_business_key 
ON billing.shipment_attributes (carrier_id, tracking_number);


-----------------------------------------------------------------------------------------------------------------

CREATE TABLE dbo.carrier_cost_ledger (
	id int IDENTITY(1,1) NOT NULL,
	carrier_invoice_number nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	shipment_date datetime NULL,
	shipment_external_id bigint NULL,
	customer_id int NULL,
	carrier_id int NOT NULL,
	shipping_method_id int NULL,
	category nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	cost_item nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	amount decimal(18,2) NOT NULL,
	charge_type_id int NULL,
	shipment_package_id int NULL,
	carrier_bill_id int NULL,
	shipment_attribute_id bigint NULL,  -- NEW: Links to physical shipment data for variance calculation
	fee_id int NULL,
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,
	carrier_invoice_date date NOT NULL,
	is_processed bit DEFAULT 0 NOT NULL,
	has_data bit DEFAULT 0 NULL,
	status varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	reason nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	note nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	status_updated_at datetime2(0) NULL,
	
	CONSTRAINT PK_carrier_cost_ledger PRIMARY KEY (id),
	
	-- Foreign Key Constraints
	CONSTRAINT FK_carrier_cost_ledger_carrier_bill 
		FOREIGN KEY (carrier_bill_id) 
		REFERENCES billing.carrier_bill(carrier_bill_id),
	
	CONSTRAINT FK_carrier_cost_ledger_charge_types 
		FOREIGN KEY (charge_type_id) 
		REFERENCES dbo.charge_types(charge_type_id),
	
	CONSTRAINT FK_carrier_cost_ledger_shipping_method 
		FOREIGN KEY (shipping_method_id) 
		REFERENCES dbo.shipping_method(shipping_method_id),
	
	CONSTRAINT FK_carrier_cost_ledger_shipment_attributes 
		FOREIGN KEY (shipment_attribute_id) 
		REFERENCES billing.shipment_attributes(id)
);

-- Unique Constraint: Prevent duplicate charges for same invoice/tracking/charge type combination
CREATE UNIQUE NONCLUSTERED INDEX UQ_carrier_cost_ledger_invoice_tracking_charge
ON dbo.carrier_cost_ledger (carrier_bill_id, tracking_number, charge_type_id)
WHERE carrier_bill_id IS NOT NULL AND charge_type_id IS NOT NULL;

-- Note: IX_carrier_cost_ledger_status_date intentionally omitted.
-- The reference design INCLUDEs variance_amount which does not exist in the current schema.

-- Index for shipment attribute FK lookup performance
CREATE NONCLUSTERED INDEX IX_carrier_cost_ledger_shipment_attribute_id
ON dbo.carrier_cost_ledger (shipment_attribute_id);

/*
================================================================================
GOLD LAYER VIEWS
================================================================================
*/

-----------------------------------------------------------------------------------------------------------------
-- View: Shipment Summary with Calculated Cost
-----------------------------------------------------------------------------------------------------------------
-- Purpose: Provides shipment attributes with billed_shipping_cost calculated from itemized charges
-- Design: Single source of truth (shipment_charges), view calculates aggregate on-the-fly
-- Performance: Fast via indexed FK relationship (shipment_attribute_id)

CREATE VIEW billing.vw_shipment_summary AS
SELECT 
    sa.id,
    sa.carrier_id,
    sa.tracking_number,
    sa.shipment_date,
    sa.shipping_method,
    sa.destination_zone,
    sa.billed_weight_oz,
    sa.billed_length_in,
    sa.billed_width_in,
    sa.billed_height_in,
    sa.created_date,
    sa.updated_date,
    -- Calculated fields from charges
    ISNULL(SUM(sc.amount), 0) AS billed_shipping_cost,
    COUNT(sc.id) AS charge_count
FROM billing.shipment_attributes sa
LEFT JOIN billing.shipment_charges sc 
    ON sc.shipment_attribute_id = sa.id
GROUP BY 
    sa.id, sa.carrier_id, sa.tracking_number, sa.shipment_date,
    sa.shipping_method, sa.destination_zone, sa.billed_weight_oz,
    sa.billed_length_in, sa.billed_width_in, sa.billed_height_in,
    sa.created_date, sa.updated_date;

-----------------------------------------------------------------------------------------------------------------
-- View: FedEx Charges (Unpivot)
-----------------------------------------------------------------------------------------------------------------
-- Purpose: Unpivots FedEx bill wide format (50 charge columns) into narrow format
-- Design: Uses OUTER APPLY to unpivot charge description/amount pairs
-- Usage: Used by Insert_Unified_tables.sql to map charges to shipment_charges table

CREATE VIEW billing.vw_FedExCharges AS
SELECT
    fb.carrier_bill_id,
    fb.invoice_number,
    fb.express_or_ground_tracking_id,
    fb.created_date,
    v.charge_type,
    v.charge_amount
FROM billing.fedex_bill fb
OUTER APPLY (
    VALUES
        ('Transportation Charge', fb.[Transportation Charge Amount]),
        (fb.[Tracking ID Charge Description], fb.[Tracking ID Charge Amount]),
        (fb.[Tracking ID Charge Description_1], fb.[Tracking ID Charge Amount_1]),
        (fb.[Tracking ID Charge Description_2], fb.[Tracking ID Charge Amount_2]),
        (fb.[Tracking ID Charge Description_3], fb.[Tracking ID Charge Amount_3]),
        (fb.[Tracking ID Charge Description_4], fb.[Tracking ID Charge Amount_4]),
        (fb.[Tracking ID Charge Description_5], fb.[Tracking ID Charge Amount_5]),
        (fb.[Tracking ID Charge Description_6], fb.[Tracking ID Charge Amount_6]),
        (fb.[Tracking ID Charge Description_7], fb.[Tracking ID Charge Amount_7]),
        (fb.[Tracking ID Charge Description_8], fb.[Tracking ID Charge Amount_8]),
        (fb.[Tracking ID Charge Description_9], fb.[Tracking ID Charge Amount_9]),
        (fb.[Tracking ID Charge Description_10], fb.[Tracking ID Charge Amount_10]),
        (fb.[Tracking ID Charge Description_11], fb.[Tracking ID Charge Amount_11]),
        (fb.[Tracking ID Charge Description_12], fb.[Tracking ID Charge Amount_12]),
        (fb.[Tracking ID Charge Description_13], fb.[Tracking ID Charge Amount_13]),
        (fb.[Tracking ID Charge Description_14], fb.[Tracking ID Charge Amount_14]),
        (fb.[Tracking ID Charge Description_15], fb.[Tracking ID Charge Amount_15]),
        (fb.[Tracking ID Charge Description_16], fb.[Tracking ID Charge Amount_16]),
        (fb.[Tracking ID Charge Description_17], fb.[Tracking ID Charge Amount_17]),
        (fb.[Tracking ID Charge Description_18], fb.[Tracking ID Charge Amount_18]),
        (fb.[Tracking ID Charge Description_19], fb.[Tracking ID Charge Amount_19]),
        (fb.[Tracking ID Charge Description_20], fb.[Tracking ID Charge Amount_20]),
        (fb.[Tracking ID Charge Description_21], fb.[Tracking ID Charge Amount_21]),
        (fb.[Tracking ID Charge Description_22], fb.[Tracking ID Charge Amount_22]),
        (fb.[Tracking ID Charge Description_23], fb.[Tracking ID Charge Amount_23]),
        (fb.[Tracking ID Charge Description_24], fb.[Tracking ID Charge Amount_24]),
        (fb.[Tracking ID Charge Description_25], fb.[Tracking ID Charge Amount_25]),
        (fb.[Tracking ID Charge Description_26], fb.[Tracking ID Charge Amount_26]),
        (fb.[Tracking ID Charge Description_27], fb.[Tracking ID Charge Amount_27]),
        (fb.[Tracking ID Charge Description_28], fb.[Tracking ID Charge Amount_28]),
        (fb.[Tracking ID Charge Description_29], fb.[Tracking ID Charge Amount_29]),
        (fb.[Tracking ID Charge Description_30], fb.[Tracking ID Charge Amount_30]),
        (fb.[Tracking ID Charge Description_31], fb.[Tracking ID Charge Amount_31]),
        (fb.[Tracking ID Charge Description_32], fb.[Tracking ID Charge Amount_32]),
        (fb.[Tracking ID Charge Description_33], fb.[Tracking ID Charge Amount_33]),
        (fb.[Tracking ID Charge Description_34], fb.[Tracking ID Charge Amount_34]),
        (fb.[Tracking ID Charge Description_35], fb.[Tracking ID Charge Amount_35]),
        (fb.[Tracking ID Charge Description_36], fb.[Tracking ID Charge Amount_36]),
        (fb.[Tracking ID Charge Description_37], fb.[Tracking ID Charge Amount_37]),
        (fb.[Tracking ID Charge Description_38], fb.[Tracking ID Charge Amount_38]),
        (fb.[Tracking ID Charge Description_39], fb.[Tracking ID Charge Amount_39]),
        (fb.[Tracking ID Charge Description_40], fb.[Tracking ID Charge Amount_40]),
        (fb.[Tracking ID Charge Description_41], fb.[Tracking ID Charge Amount_41]),
        (fb.[Tracking ID Charge Description_42], fb.[Tracking ID Charge Amount_42]),
        (fb.[Tracking ID Charge Description_43], fb.[Tracking ID Charge Amount_43]),
        (fb.[Tracking ID Charge Description_44], fb.[Tracking ID Charge Amount_44]),
        (fb.[Tracking ID Charge Description_45], fb.[Tracking ID Charge Amount_45]),
        (fb.[Tracking ID Charge Description_46], fb.[Tracking ID Charge Amount_46]),
        (fb.[Tracking ID Charge Description_47], fb.[Tracking ID Charge Amount_47]),
        (fb.[Tracking ID Charge Description_48], fb.[Tracking ID Charge Amount_48]),
        (fb.[Tracking ID Charge Description_49], fb.[Tracking ID Charge Amount_49]),
        (fb.[Tracking ID Charge Description_50], fb.[Tracking ID Charge Amount_50])
) v (charge_type, charge_amount)
WHERE NULLIF(v.charge_type, '') IS NOT NULL
  AND v.charge_amount IS NOT NULL
  AND v.charge_amount <> 0;