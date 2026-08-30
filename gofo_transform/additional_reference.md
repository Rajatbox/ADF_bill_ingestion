1. would have to fabricate invoice number (It's there in invoice but not easily fetchable so gonna design a rule around it to reproduce it): 

If you can see it's in the very first column and has a pattern i.e: 
Customer ID + YYYYMM + 00(1 or 2 : This depends on the date range we have file for, gofo sends only two invoice per month, 1-15 and 16-31)
So we will pull out the max(shipment_Date) if <= 15 then 1 and if > 15 then 2)

2. For invoice date i will make it Max(Shipment_date). 
3. There are a lot of missing shipment method so will setup a rule for swapping them with it's standard one.

4. Dimensions are a bit tricky so check out UPS i think we get somewhat similar in there as well 
