/*
================================================================================
Database Schema Definition
================================================================================
Note: Database name is omitted - will be parameterized via ADF Linked Service
      per environment (DEV/UAT/PROD). Use only schema.table format.
================================================================================
*/

CREATE TABLE test.delta_fedex_bill (
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
	RMA# varchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
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


-----------------------------------------------------------------------------------------------------------------


CREATE TABLE Test.fedex_bill (
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
		REFERENCES Test.carrier_bill(carrier_bill_id)
);

-- Index for FK lookup performance (join with carrier_bill)
CREATE NONCLUSTERED INDEX IX_fedex_bill_carrier_bill_id
ON Test.fedex_bill (carrier_bill_id);

-- Index for incremental processing (used by Insert_Unified_tables.sql)
CREATE NONCLUSTERED INDEX IX_fedex_bill_created_date
ON Test.fedex_bill (created_date);

-- Composite index for tracking number lookups (used in mapping queries)
CREATE NONCLUSTERED INDEX IX_fedex_bill_tracking_number_invoice
ON Test.fedex_bill (express_or_ground_tracking_id, invoice_number, invoice_date);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE Test.carrier_bill (
	carrier_bill_id int IDENTITY(1,1) NOT NULL,
	bill_id int NULL,
	carrier_id int NULL,
	bill_date date NULL,
	total_amount decimal(18,2) NULL,
	num_shipments int NULL,
	reconciled_amount decimal(18,2) NULL,
	discrepancy_amount decimal(18,2) NULL,
	bill_number varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	CONSTRAINT PK__carrier___A6B346871A6B3E58 PRIMARY KEY (carrier_bill_id)
);

-- Unique constraint: Prevent duplicate invoices per carrier
CREATE UNIQUE NONCLUSTERED INDEX UQ_carrier_bill_number_date_carrier
ON Test.carrier_bill (bill_number, bill_date, carrier_id)
WHERE carrier_id IS NOT NULL AND bill_date IS NOT NULL;

-- Index for carrier-based queries
CREATE NONCLUSTERED INDEX IX_carrier_bill_carrier_id
ON Test.carrier_bill (carrier_id, bill_date);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE test.shipping_method (
	shipping_method_id int IDENTITY(1,1) NOT NULL,
	carrier_id int NOT NULL,
	method_name varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	service_level varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	average_transit_days decimal(5,2) NULL,
	guaranteed_delivery bit NOT NULL,
	is_active bit NOT NULL,
	name_in_bill varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK_shipping_method PRIMARY KEY (shipping_method_id)
);

-- Unique constraint: Prevent duplicate shipping methods per carrier
CREATE UNIQUE NONCLUSTERED INDEX UQ_shipping_method_carrier_name
ON test.shipping_method (carrier_id, method_name);

-- Index for carrier-based lookups
CREATE NONCLUSTERED INDEX IX_shipping_method_carrier_active
ON test.shipping_method (carrier_id, is_active);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE test.charge_types (
	charge_type_id int IDENTITY(1,1) NOT NULL,
	carrier_id int NOT NULL,
	charge_name varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	freight bit NULL,
	dt bit NULL,
	markup bit NULL,
	category varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	charge_category_id int NULL,
	CONSTRAINT PK__charge_t__3BEF5FDF18D7DC05 PRIMARY KEY (charge_type_id)
);

-- Unique constraint: Prevent duplicate charge types per carrier
CREATE UNIQUE NONCLUSTERED INDEX UQ_charge_types_carrier_name
ON test.charge_types (carrier_id, charge_name);

-- Index for category-based queries
CREATE NONCLUSTERED INDEX IX_charge_types_carrier_category
ON test.charge_types (carrier_id, charge_category_id);

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE Test.shipment_charges (
	id int IDENTITY(1,1) NOT NULL,
	carrier_id int NOT NULL,
	carrier_bill_id int NOT NULL,
	tracking_number nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	charge_type_id int NOT NULL,
	amount decimal(18,2) NULL,
	shipment_attribute_id int NOT NULL,
	created_date datetime2 DEFAULT sysdatetime() NOT NULL,
	CONSTRAINT PK__shipment__charges PRIMARY KEY (id),
	CONSTRAINT FK_shipment_charges_attributes FOREIGN KEY (shipment_attribute_id) 
		REFERENCES Test.shipment_attributes(id)
);

-- Index for FK lookup performance
CREATE NONCLUSTERED INDEX IX_shipment_charges_attribute_id
ON Test.shipment_charges (shipment_attribute_id);

-- Unique constraint: Prevent duplicate charges for same shipment/carrier_bill/charge_type
CREATE UNIQUE NONCLUSTERED INDEX UQ_shipment_charges_bill_tracking_charge
ON Test.shipment_charges (carrier_bill_id, tracking_number, charge_type_id);

-- Index for carrier-based queries
CREATE NONCLUSTERED INDEX IX_shipment_charges_carrier_date
ON Test.shipment_charges (carrier_id, created_date);


-----------------------------------------------------------------------------------------------------------------


CREATE TABLE Test.shipment_attributes (
	id int IDENTITY(1,1) NOT NULL,
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
ON Test.shipment_attributes (carrier_id, tracking_number);


-----------------------------------------------------------------------------------------------------------------
-- View: Shipment Summary with Calculated Cost
-----------------------------------------------------------------------------------------------------------------
-- Purpose: Provides shipment attributes with billed_shipping_cost calculated from itemized charges
-- Design: Single source of truth (shipment_charges), view calculates aggregate on-the-fly
-- Performance: Fast via indexed FK relationship (shipment_attribute_id)

CREATE VIEW Test.vw_shipment_summary AS
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
FROM Test.shipment_attributes sa
LEFT JOIN Test.shipment_charges sc 
    ON sc.shipment_attribute_id = sa.id
GROUP BY 
    sa.id, sa.carrier_id, sa.tracking_number, sa.shipment_date,
    sa.shipping_method, sa.destination_zone, sa.billed_weight_oz,
    sa.billed_length_in, sa.billed_width_in, sa.billed_height_in,
    sa.created_date, sa.updated_date


-----------------------------------------------------------------------------------------------------------------

CREATE TABLE Test.carrier_cost_ledger (
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
	shipment_attribute_id int NULL,  -- NEW: Links to physical shipment data for variance calculation
	is_matched bit DEFAULT 0 NOT NULL,
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
		REFERENCES Test.carrier_bill(carrier_bill_id),
	
	CONSTRAINT FK_carrier_cost_ledger_charge_types 
		FOREIGN KEY (charge_type_id) 
		REFERENCES test.charge_types(charge_type_id),
	
	CONSTRAINT FK_carrier_cost_ledger_shipping_method 
		FOREIGN KEY (shipping_method_id) 
		REFERENCES test.shipping_method(shipping_method_id),
	
	CONSTRAINT FK_carrier_cost_ledger_shipment_attributes 
		FOREIGN KEY (shipment_attribute_id) 
		REFERENCES Test.shipment_attributes(id)
);

-- Unique Constraint: Prevent duplicate charges for same invoice/tracking/charge type combination
CREATE UNIQUE NONCLUSTERED INDEX UQ_carrier_cost_ledger_invoice_tracking_charge
ON Test.carrier_cost_ledger (carrier_bill_id, tracking_number, charge_type_id)
WHERE carrier_bill_id IS NOT NULL AND charge_type_id IS NOT NULL;

-- Index for reconciliation queries (filter by status and date)
CREATE NONCLUSTERED INDEX IX_carrier_cost_ledger_status_date
ON Test.carrier_cost_ledger (status, carrier_invoice_date)
INCLUDE (is_matched, is_processed, amount, variance_amount);

-- Index for shipment attribute FK lookup performance
CREATE NONCLUSTERED INDEX IX_carrier_cost_ledger_shipment_attribute_id
ON Test.carrier_cost_ledger (shipment_attribute_id);