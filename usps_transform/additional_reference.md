1. tranType = charge type 
mapping for fist seed: 
ADJUSTMENT -> charge_name = "ADJ - {assessmentType}" (falls back to "ADJ - UNKNOWN" if assessmentType is blank/null), category = ADJUSTMENT
  USPS reports many distinct adjustment reasons (postage delta, dimensional, zone, unused label, etc.)
  under a single tranType = ADJUSTMENT, so assessmentType splits them into their own charge types
  instead of collapsing into one generic "Adjustment" bucket.
REFUND = REFUND category = Other 
PURCHASE = POSTAGE category = transportation 
Everything else goes to category = other 


2. tranDate = shipment date
3. Invoice number = USPS_tran_{min(tranDate)}_{max(tranDate)}
4. Bill date = {max(tranDate)}

4. Shipment attributes doesn't make much sense here so nothing goes in there except shipment date = {min(tranDate)} group by tracking_number 

5. pic = tracking_number

6. All columns to exist in bill table 

7. treat "postage" as amount while pushing in shipment_charges
