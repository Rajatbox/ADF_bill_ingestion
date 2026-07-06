This is a pivoted type of schema like Fedex with just 3 charge type columns so no need for a seperate view just use corss apply. 

The shipment method is a bit tricky here, we have limited data so according to it we will have to fetch it from the "Charge Name 1" where "Charge Code 1" <> 'ADC'

For Account number use prop_0

For shipment date use "Created Timestamp"

This is a standalone carrier 
