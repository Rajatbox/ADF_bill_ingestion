The invoice number and invoice date would be pulled from the header record and would come at the last of the csv while ingesting from adf, so make sure to include those in delta schema. 

DHL
Here is an example bill
A lot of blank columns in between the columns that we need
The first line of the file is not the header, it actually contains important summary information.
Cell B1 in the spreadsheet represents bill date
Cell C1 is invoice number
Cell D1 is account number
Cells G1:K1 shows the warehouse address, can be used to verify the upload matches the correct account and warehouse 
Cell L1 shows the total bill amount
Cell N1 shows the number of shipments on the bill
Then there is not a header/column name row, the data just starts in the second row
Each shipment will have two lines, but the second line doesn’t add any value, so really it’s just each shipment is represented on a single row
Different columns have each charge type and amounts
There can be both domestic and international shipments on the same bill, and there are different
The total charge amount for the shipment is the sum of columns 30, 48, 65, and 79
Need to verify on each bill upload that SUM(column 30, 48, 65, and 79) == Cell L1
Falcon currently only ships DDU with DHL, so there are no duties and taxes on the bill for DHL international shipments. So I’m not
There used to be a DHL International upload, they now get them in the same file, but we should still prepare for DHL international upload if other customers have them

Column 1:
Column 2: Account Number
Column 3:
Column 4:
Column 5:
Column 6:
Column 7:
Column 8:
Column 9: Shipping Date
Column 10:
Column 11:
Column 12: Tracking Number (for International Shipments Only)
Column 13: unique_id (for Domestic Shipments Only)
This is the tracking number column, but it’s technically only part of it. The way you get domestic tracking numbers from DHL bills is by combining “420” + the first 5 digits of zip code (leading zeroes included if it’s a 4 digit zip code) + unique_id column
Column 14:
Column 15: Recipient Address Line 1
Column 16: Recipient Address Line 2
Column 17: Recipient City
Column 18: Recipient State/Province
Column 19: Recipient Zip/Postal Code
Column 20: Recipient Country
Can use country to indicate domestic or international shipment
Column 21:
Column 22: Shipping Method
Column 23: Shipped Weight
Column 24: Shipped Weight Unit of Measure
Column 25: Billed Weight
Column 26: Billed Weight Unit of Measure
Column 27:
Column 28:
Column 29: Zone
Column 30: Transportation Cost
Column 31:
Column 32:
Column 33:
Column 34:
Column 35:
Column 36:
Column 37:
Column 38:
Column 39:
Column 40:
Column 41:
Column 42:
Column 43:
Column 44:
Column 45:
Column 46:
Column 47:
Column 48: Non-Qualified Dimensional Charges
Column 49:
Column 50:
Column 51:
Column 52:
Column 53:
Column 54:
Column 55:
Column 56:
Column 57:
Column 58:
Column 59:
Column 60:
Column 61:
Column 62:
Column 63:
Column 64:
Column 65: Fuel Surcharge Amount
Column 66:
Column 67: Overlabel Tracking Number
Column 68:
Column 69:
Column 70:
Column 71:
Column 72:
Column 73:
Column 74:
Column 75:
Column 76:
Column 77:
Column 78:
Column 79: Delivery Area Surcharge Amount


DHL International

