UPS
A ton of columns, and a lot of them are useless
Each shipment is broken out into its different line items on the bill. So a bill might have 1k shipments, but 4-5k rows in the csv/excel file.
Here is an example bill
Here is the header row (I already put the header row in the example bill, they will NOT already have this when you just get a bill)
The sum of each line items costs will give the total cost for the shipment, but it’s easier to get the individual charges
The csv/xslx file from the carrier does not have a header row/column names, so you have to add that in before you process the file, or you can just go by the column index if that’s easier
I will typically ingest the entire file as a dataframe or table, and do some data cleanup, upload to raw UPS table, and then do operations to get charge types and then group on tracking number to get total shipping cost and correct data for shipments table
Here is a simple way of explaining the charge types/categories


—-- UPS Charge Type Explanations —--

These 5 tiers below represent the columns on the spreadsheet, and which pairs/combinations i’ve seen before, as they appear in the columns of the UPS Bill

Charge Category Code
	Charge Category Detail Code
		Charge Classification Code
			Charge Description Code
Charge Description
				

ADJ: Post Delivery Adjustments
	
ADC: Address Correction
		FRT: Transportation Charges
			1
				Address Correction Next Day Air
			3
				Address Correction Ground
			8
				Address Correction Expedited
			13	
				Address Correction Next Day Air Saver
	
CADJ: Credit Adjustment
		MSC: Miscellaneous Charges
			(Blank)
				Billing Adjustment for W/E {MM/DD/YYYY}
	
CLB: Closed-Loop Billing
		ACC: Accessorial Charges
			NPF
				Not Previously Billed Missing PLD Fee
			PSC
				Not Previously Billed Demand Surcharge-Com
			PSR
				Not Previously Billed Demand Surcharge-Resi
			RDC
				Not Previously Billed Delivery Area Surcharge (Com)
			RDR
				Not Previously Billed Delivery Area Surcharge (Resi)
			RES
				Not Previously Billed Residential Surcharge
			RS1
				Not Previously Billed Canada Residential Surcharge
		FRT: Transportation Charges
			3
				Not Previously Billed Ground Commercial
				Not Previously Billed Ground Residential
			93
				Not Previously Billed UPS SurePost - 1lb or Greater
		FSC: Fuel Surcharges
			FSC
				Not Previously Billed Fuel Surcharge

DIN: Shipper-Initiated Intercept
	ACC: Accessorial Charges
		IRS
			Return To Sender - Phone Request
		IRW
			Reroute - Web Request
		ISW
			Return To Sender - Web Request
		RDC
			Delivery Area Surcharge
	FRT: Transportation Charges
		3
			Ground Return to Sender
		12
			3 Day Select Return to Sender
	FSC: Fuel Surcharges
		FSC
			Fuel Surcharge

FPOD: Fax Proof of Delivery
	FRT: Transportation Charges
		(Blank)
			(Blank)

RADJ: Residential Adjustments
	ACC: Accessorial Charges
		CSG
			Delivery Confirmation Signature - Commercial Adjustment
		DCS
			Delivery Confirmation Signature Adjustment
		LDC
			Delivery Area Surcharge - Extended Adjustment (Com)
		LDR
			Delivery Area Surcharge - Extended Adjustment (Resi)
		RDC
			Delivery Area Surcharge Adjustment (Com)
		RDR
			Delivery Area Surcharge Adjustment (Resi)
		RES
			Residential Surcharge Adjustment
	FRT: Transportation Charges
		COM
			Commercial Adjustment
		RES
			Residential Adjustment
	FSC: Fuel Surcharges
		FSC
			Fuel Surcharge Adjustment

SCC: Shipping Charge Correction
	ACC: Accessorial Charges
		AHC
			Shipping Charge Correction Additional Handling
		AHG
			Shipping Charge Correction Additional Handling - Length+Girth
		NCB
			Shipping Charge Correction Non-Standard Cube
		NLN
			Shipping Charge Correction Non-Standard Length
		SAH
			Shipping Charge Correction Demand Surcharge-Addl Handling
	FRT: Transportation Charges
		1
			Shipping Charge Correction Next Day Air
		2
			Shipping Charge Correction 2nd Day Air
			3
				Shipping Charge Correction Ground
				Shipping Charge Correction Ground Undeliverable Return
Shipping Charge Correction Ground Return to Sender
			12
				Shipping Charge Correction 3 Day Select
				Shipping Charge Correction 3 Day Select Undeliverable Return
				Shipping Charge Correction 3 Day Select Return to Sender
			13
				Shipping Charge Correction Next Day Air Saver
			14
				Shipping Charge Correction Next Day Air Early
			92
				Shipping Charge Correction UPS SurePost - Less than 1 LB
			93
				Shipping Charge Correction UPS SurePost - 1 LB or Greater
			(Blank)
				Shipping Charge Correction
	FSC: Fuel Surcharges
		FSC
			Shipping Charge Correction Fuel Surcharge

ZONE: Zone
	FRT: Transportation Charges
		3
			ZONE ADJUSTMENT Ground
		13
			ZONE ADJUSTMENT Next Day Air Saver
	FSC: Fuel Surcharges
		FSC
			ZONE ADJUSTMENT Fuel Surcharge

MIS: Miscellaneous Charges
SVCH: Service Charge
		FRT: Transportation Charges
			(Blank)
				Service Charge
RTN: Returns
	RTS: Undeliverable Return
		ACC: Accessorial Charges
			RDC
				Delivery Area Surcharge
		BRK: Brokerage Charges
			389
				PGA Disclaim Fee
			405
				Disbursement Fee
			412
				FDA Clearance
		FRT: Transportation Charges
			3
				Ground Undeliverable Return
			8
				WW Expedited Undeliverable Return
			12
				3 Day Select Undeliverable Return
		FSC: Fuel Surcharges
			FSC
				Fuel Surcharge
		GOV: Government Charges
			201
				Duty Amount
			212
				Merchandise Processing Fee
		INF: Informational
			3
				Ground Undeliverable Return
			12
				3 Day Select Undeliverable Return
SHP: Transportation
	
DF:
		ACC: Accessorial Charges
			96
				OVERSIZE FEE
			D17
				LIFTGATE SURCHARGE - DEST
		FRT: Transportation Charges
			AF
				AIR FREIGHT
		FSC: Fuel Surcharges
			33
				FUEL-INDEX
		TBD: ??
			83
				US EXPORT COMPLIANCE FEE
			387
				RESIDENTIAL PICKUP
			D99
				SECURITY SURCHARGE
	
EG:
		ACC: Accessorial Charges
			40
				RESIDENTIAL DELIVERY
			41
				DDP/DDU SERVICE FEE
			84
				SECURITY FEE
			AD
				ADVANCE AT DESTINATION
		FRT: Transportation Charges
			AF
				AIR FREIGHT
		FSC: Fuel Surcharges
			33
				FUEL-INDEX
			488
				CARTAGE FUEL SURCHARGE - DESTINATION
		TBD: ??
			71
				IMPORT SERVICE FEE
			83
				US EXPORT COMPLIANCE FEE	
			621
				ALFA COST / AIRLINE HANDLING
			700
				IMPORT HANDLING
			936
				DELIVERY ORDER
			DL
				DELIVERY
			E01
				REGULATORY FEE
			PU
				PICKUP
			TC
				TERMINAL SERVICE CHARGE
	
FC: Freight Collect
		FRT: Transportation Charges
			3
				Ground Commercial Collect
		FSC: Fuel Surcharges
			FSC
				Fuel Surcharge
	
IMP: Imports
		ACC: Accessorial Charges
			AHC
				Additional Handling Charge
			F/D
				Duty and Tax Forwarding Surcharge
			FRT
				Freight
		BRK: Brokerage Charges
			405
				Disbursement Fee
			426
				Brokerage GST
		FRT: Transportation Charges
			8
				WW Expedited
		FSC: Fuel Surcharges
			FSC
				Fuel Surcharge
		GOV: Government Charges
			201
				Duty Amount
			205
				Value Added Tax
			206
				Customs Gst
			214
				Ca Customs Hst
			235
				Ca British Columbia Pst
		(Blank): Data Unavailable
			(Blank)
				(Blank)
	
ISS: Internet Shipping
		ACC: Accessorial Charges
			ADS
				Adult Signature Required
			AHG
				Additional Handling - Length+Girth
			AHL
				Addl. Handling longest side
			AHW
				Addl. Handling weight
			AKC
				Remote Area Surcharge
			AKR
				Remote Area Surcharge
			CAC
				Remote Area Surcharge
			CAR
				Remote Area Surcharge
			CSG
				Delivery Confirmation Signature - Commercial
			DCS
				Delivery Confirmation Signature
			HIC
				Remote Area Surcharge (Com)
			HIR
				Remote Area Surcharge (Resi)
			LDC
				Delivery Area Surcharge - Extended (Com)
			LDR
				Delivery Area Surcharge - Extended (Resi)
			NCB
				Non-Standard Cube
			NLN
				Non-Standard Length
			RDC
				Delivery Area Surcharge (Com)
			RDR
				Delivery Area Surcharge (Resi)
			RES
				Residential Surcharge
			SAH
				Demand Surcharge-Addl Handling
			SAT
				Saturday Delivery
			SDA
				Delivery Area Surcharge
			SDE
				Delivery Area Surcharge - Extended
			SUR
				Early Surcharge
		FRT: Transportation Charges
			1
				Next Day Air Commercial
				Next Day Air Commercial Third Party
				Next Day Air Residential
				Next Day Air Residential Third Party
			2
				2nd Day Air Commercial
				2nd Day Air Commercial Third Party
				2nd Day Air Residential
2nd Day Air Residential Third Party
			3
				Ground Commercial
				Ground Hundredweight Third Party
				Ground Residential
				Ground Residential Third Party
			12
				3 Day Select Commercial
				3 Day Select Residential
			13
				Next Day Air Saver Commercial
				Next Day Air Saver Residential
			14
				Next Day Air Early Commercial
				Next Day Air Early Residential
			92
				UPS SurePost - Less than 1 lb
			93
				UPS SurePost - 1lb or Greater
		FSC: Fuel Surcharges
			FSC
				Fuel Surcharge
		INF: Informational
			1
				Next Day Air Commercial	
				Next Day Air Residential
				Next Day Air Residential Third Party
			2
				2nd Day Air Commercial
				2nd Day Air Residential
				2nd Day Air Residential Third Party
			3
				Ground Commercial
				Ground Residential
				Ground Residential Third Party
			12
				3 Day Select Commercial
				3 Day Select Residential
			13
				Next Day Air Saver Commercial
				Next Day Air Saver Residential
			14
				Next Day Air Early Commercial
				Next Day Air Early Residential
			92
				UPS SurePost - Less than 1 lb
			93
				UPS Surepost - 1lb or Greater
			AHG
				Additional Handling - Length+Girth
			AHL
				Addl. Handling longest side
			AHW
				Addl. Handling weight
			FSC
				Fuel Surcharge
			RDC
				Delivery Area Surcharge
			RES
				Residential Surcharge
			SAH
				Demand Surcharge-Addl Handling
	
WWS: Worldwide Service
		ACC: Accessorial Charges
			ADS
				Adult Signature Required
			DCS
				Delivery Confirmation Signature
			ESD
				Extended Area Surcharge
			PSC
				Demand Surcharge-Com
			PSR
				Demand Surcharge-Resi
			RES
				Residential Surcharge
			SED
				Electronic Export Information Fee
		FRT: Transportation Charges
			3
				Standard Shipment
				Standard to Canada
			7
				Worldwide Express
				Worldwide Express Shipment
			8
				Worldwide Expedited
				Worldwide Expedited Shipment
			69
				Worldwide Saver
		FSC: Fuel Surcharges
			FSC
				Fuel Surcharge
		INF: Informational
			3
				Standard Shipment
				Standard to Canada
			8
				Worldwide Shipment
				Worldwide Expedited Shipment
			69
				Worldwide Saver


—-- Columns —--

Version
Recipient Number
Account Number
Account Country
Invoice Date
Invoice Number
Invoice Type Code
Invoice Type Detail Code
Account Tax ID
Invoice Currency Code
Invoice Amount
Transaction Date
Pickup Record Number
Lead Shipment Number
The lead or master tracking number for a multi-piece shipment
World Ease Number
Shipment Reference Number 1
Shipment Reference Number 2
Bill Option Code
Package Quantity
Oversize Quantity
Tracking Number
Package Reference Number 1
Package Reference Number 2
Package Reference Number 3
Package Reference Number 4
Package Reference Number 5
Entered Weight
Entered Weight Unit of Measure
Can be either L, K or O (pounds, kilograms, or ounces), needs to be converted to ounces where necessary
Billed Weight
Billed Weight Unit of Measure
Can be either L, K or O (pounds, kilograms, or ounces), needs to be converted to ounces where necessary
Container Type
Typically PKG for “Package”, but there are lots of other possibilities that we probably don’t need to consider or worry about yet
Billed Weight Type
0: No weight information available
1: Actual Weight
2: Dimensional Weight
3: Minimum Weight
4: Maximum Weight
5: Oversize 1 Weight
6: Keyed Actual Weight
7: Keyed Dimensional Weight
8: Scanned Actual Weight
9: Scanned Dimensional Weight
10: Dimensional Weight
11: Dimensional Weight
12: Default Weight
13: Oversize 2 Weight
14: Median Weight
15: Average Weight
16: Driver Supplied Weight
17: Scanned Weight
18: Void Weight
19: Customer Supplied Weight
20: Scale Weight
21: Oversize 3 Weight
22: Keyed Dimensional Weight
23: Total Billable Weight
24: Total Billable Weight
25: LPS Minimum Billable Weight
26: LPS Minimum Billable Weight
27: Scanned LPS Minimum Billable Weight
28: Scanned LPS Minimum Billable Weight
29: Blended Package Weight
30: Keyed package weight (Playback)
31: Keyed blended actual weight
32: Keyed blended dimensional weight 
33: Overage weight based on scan actual
34: Overage weight based on scan dimensional
35: Overage weight based on scan incented dimensional
36: Balloon Weight
37: Oversized Weight
38: Over Maximum Weight
Package Dimensions
Format is like 12.0x 9.0x 7.0 or 9.0x 8.0x 5.0
(Length)x (Width)x (Height)
Zone
Typically 1-9, but UPS can do it like 204 or 306, which represents zone 4 or 6, they just prefix with 20 or 30 to indicate the shipping methods used.
Charge Category Code
Charge Category Detail Code
Even different Charge Category Detail Codes can have the same Charge Classification Codes and Charge Description Codes
SRB: Standard Recording Book
CWT1: Hundredweight
CWT2: Hundredweight
ASD: Air Shipping Document
WWS: World Wide Service
GCC: UPS WorldEase
OTPU: One-Time Pickup Request
EXPC: UPS Express Critical
ISS: Internet Shipping
CHBK: Chargeback
ECOD: Express COD
FC: Freight Collect
TP: Third Party
GCTP: UPS WorldEase Third Party
CBS: Consignee Billing
ARS: Authorized Return Service
RS: Return Service
ROW: Returns on the Web
RSPU: Return Service Pickup
IBS: Information Based Services
PSUP: Package Supplies
HAZ: Hazardous Materials
RTS: Undeliverable Return
CLB: Closed-Loop Billing
MISC: Miscellaneous
FPOD: Fax Proof of Delivery
CADJ: Credit Adjustment
RADJ: Residential Adjustments
SCC: Shipping Charge Correction
OCA: On Call Adjustments
OCG: On Call Request
DCON: Delivery Confirmation
CTAG: Call Tags
CTGR: Call Tag Refund
AGSR: Automated GSR
GSR: GSR
FEES: Fees
VOID: Voids
SVCH: Service Charge
SRM: Shipping Record Manifest
MAN: Manifest
DSD: Domestic Shipping Document
ISD: International Shipping Document
NSD: Non-EU Waybill
FCTP: Freight Collect Third Party
ADC: Address Correction
ADJ: Adjustment
DIN: Shipper-Initiated Intercept
IMP: Imports
ZONE: Zone
IC: Import Control
PDPO: UPS My Choice Options
PDPS: UPS My Choice Premium
Charge Source
Type Code 1
Type Detail Code 1
Type Detail Value 1
Type Code 2
Type Detail Code 2
Type Detail Value 2
Charge Classification Code
FRT: Transportation Charges
GOV: Government Charges
BRK: Brokerage Charges
ACC: Accessorial Charges
FSC: Fuel Surcharges
TAX: Taxes
EXM: Exemptions
MSC: Miscellaneous Charges
INF: Informational
(Blank): Data Unavailable
Charge Description Code
Charge Description
Charged Unit Quantity
Basis Currency Code
Basis Value
Tax Indicator
A __ in this column indicates a duties and taxes charge. I’m a little unclear whether this is for the entire grouping of tracking numbers or just this line
Transaction Currency Code
Incentive Amount
The discount from published pricing that is given on the charge. I’m not sure if we’re going to store published amounts and discounts, or just the rates charged
Net Amount
The amount actually paid by the 3PL to the carrier for the line item charge. By summing the net amount and grouping on tracking number
Miscellaneous Currency Code
Miscellaneous Incentive Amount
Miscellaneous Net Amount
Alternate Invoicing Currency Code
Alternate Invoice Amount
Invoice Exchange Rate
Tax Variance Amount
Currency Variance Amount
Invoice Level Charge
Invoice Due Date
Alternate Invoice Number
Store Number
Customer Reference Number
Sender Name
Sender Company Name
Might be useful to store later, to help with finding unknown shipments
Sender Address Line 1
Sender Address Line 2
Sender City
Sender State
We can use this city and state to verify that the right warehouse/account is tied to this upload
Sender Postal
Sender Country
Receiver Name
Receiver Company Name
Receiver Address Line 1
Receiver Address Line 2
Receiver City
Receiver State
Receiver Postal
Receiver Country
Third Party Name
Third Party Company Name
Third Party Address Line 1
Third Party Address Line 2
Third Party City
Third Party State
Third Party Postal
Third Party Country
Sold To Name
Sold To Company Name
Sold To Address Line 1
Sold To Address Line 2
Sold To City,Sold To State
Sold To Postal
Sold To Country
Miscellaneous Address Qual 1
Miscellaneous Address 1 Name
Miscellaneous Address 1 Company Name
Miscellaneous Address 1 Address Line 1
Miscellaneous Address 1 Address Line 2
Miscellaneous Address 1 City
Miscellaneous Address 1 State
Miscellaneous Address 1 Postal
Miscellaneous Address 1 Country
Miscellaneous Address Qual 2
Miscellaneous Address 2 Name
Miscellaneous Address 2 Company Name
Miscellaneous Address 2 Address Line 1
Miscellaneous Address 2 Address Line 2
Miscellaneous Address 2 City
Miscellaneous Address 2 State
Miscellaneous Address 2 Postal
Miscellaneous Address 2 Country
Shipment Date
Shipment Export Date
Shipment Import Date
Entry Date
Direct Shipment Date
Shipment Delivery Date
Shipment Release Date
Cycle Date
EFT Date
Validation Date
Entry Port
Entry Number
Export Place
Shipment Value Amount
Shipment Description
Entered Currency Code
Customs Number
Exchange Rate
Master Air Waybill Number
EPU
Entry Type
CPC Code
Line Item Number
Goods Description
Entered Value
Duty Amount
Weight
Unit of Measure
Item Quantity
Item Quantity Unit of Measure
Import Tax ID
Declaration Number
Carrier Name
CCCD Number
Cycle Number
Foreign Trade Reference Number
Job Number
Transport Mode
Tax Type
Tariff Code
Tariff Rate
Tariff Treatment Number
Contact Name
Class Number
Document Type
Office Number
Document Number
Duty Value
Total Value for Duty
Excise Tax Amount
Excise Tax Rate
GST Amount
GST Rate
Order In Council
Origin Country
SIMA Access
Tax Value
Total Customs Amount
Miscellaneous Line 1
Miscellaneous Line 2
Miscellaneous Line 3
Miscellaneous Line 4
Miscellaneous Line 5
Payor Role Code
Indicates who is responsible for paying for the charges
01: Shipper
02: Receiver
12: Third Party (Third Party - Shipper)
12: Third Party (Third Party - Receiver)
Blank: Blank
Miscellaneous Line 7
Miscellaneous Line 8
Miscellaneous Line 9
Miscellaneous Line 10
Miscellaneous Line 11
Duty Rate
VAT Basis Amount
VAT Amount
VAT Rate
Other Basis Amount
Other Amount
Other Rate
Other Customs Number Indicator
Other Customs Number
Customs Office Name
Package Dimension Unit Of Measure
Original Shipment Package Quantity
Place Holder 24
Place Holder 25
Place Holder 26
Place Holder 27
Place Holder 28
Place Holder 29
Place Holder 30
Place Holder 31
BOL # 1
BOL # 2
BOL # 3
BOL # 4
BOL # 5
PO # 1
PO # 2
PO # 3
PO # 4
PO # 5
PO # 6
PO # 7
PO # 8
PO # 9
PO # 10
NMFC
Detail Class
Freight Sequence Number
Declared Freight Class
Place Holder 34
Place Holder 35
Place Holder 36
Place Holder 37
Place Holder 38
Place Holder 39
Place Holder 40
Place Holder 41
Place Holder 42
Place Holder 43
Place Holder 44
Place Holder 45
Place Holder 46
Place Holder 47
Place Holder 48
Place Holder 49
Place Holder 50
Place Holder 51
Place Holder 52
Place Holder 53

