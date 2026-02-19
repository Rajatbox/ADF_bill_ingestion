## Column Mapping Reference

| CSV Column Name (Source)              | Delta Table Column (Target)         | Data Type    | Notes                          |
| ------------------------------------- | ----------------------------------- | ------------ | ------------------------------ |
| `Time (UTC)`                          | `time_utc`                          | VARCHAR(50)  | Timestamp of shipment creation |
| `ID`                                  | `shipment_id`                       | VARCHAR(255) | Unique shipment identifier     |
| `USER`                                | `user_account`                      | VARCHAR(255) | Account/user name              |
| `Tracking`                            | `tracking_number`                   | VARCHAR(255) | Carrier tracking number        |
| `Status`                              | `status`                            | VARCHAR(50)  | Shipment status                |
| `Carrier`                             | `carrier`                           | VARCHAR(50)  | Carrier name (USPS)            |
| `Service`                             | `service`                           | VARCHAR(255) | Service method                 |
| `Reference`                           | `reference`                         | VARCHAR(255) | Reference number               |
| `Shipment Weight (Oz)`                | `shipment_weight_oz`                | VARCHAR(50)  | Weight in ounces               |
| `Shipment Dryice Weight (Oz)`         | `shipment_dryice_weight_oz`         | VARCHAR(50)  | Dry ice weight                 |
| `Package Type`                        | `package_type`                      | VARCHAR(50)  | Package type                   |
| `Package Length (In)`                 | `package_length_in`                 | VARCHAR(50)  | Length in inches               |
| `Package Width (In)`                  | `package_width_in`                  | VARCHAR(50)  | Width in inches                |
| `Package Height (In)`                 | `package_height_in`                 | VARCHAR(50)  | Height in inches               |
| `From Name`                           | `from_name`                         | VARCHAR(255) | Sender name                    |
| `From Company`                        | `from_company`                      | VARCHAR(255) | Sender company                 |
| `From Street`                         | `from_street`                       | VARCHAR(255) | Sender street address          |
| `From Apt/Suite`                      | `from_apt_suite`                    | VARCHAR(50)  | Sender apt/suite               |
| `From City`                           | `from_city`                         | VARCHAR(100) | Sender city                    |
| `From State`                          | `from_state`                        | VARCHAR(50)  | Sender state                   |
| `From Postal`                         | `from_postal`                       | VARCHAR(50)  | Sender postal code             |
| `From Country`                        | `from_country`                      | VARCHAR(10)  | Sender country                 |
| `To Name`                             | `to_name`                           | VARCHAR(255) | Recipient name                 |
| `To Company`                          | `to_company`                        | VARCHAR(255) | Recipient company              |
| `To Street`                           | `to_street`                         | VARCHAR(255) | Recipient street address       |
| `To Apt/Suite`                        | `to_apt_suite`                      | VARCHAR(50)  | Recipient apt/suite            |
| `To City`                             | `to_city`                           | VARCHAR(100) | Recipient city                 |
| `To State`                            | `to_state`                          | VARCHAR(50)  | Recipient state                |
| `To Postal`                           | `to_postal`                         | VARCHAR(50)  | Recipient postal code          |
| `To Country`                          | `to_country`                        | VARCHAR(10)  | Recipient country              |
| `First Scan`                          | `first_scan`                        | VARCHAR(50)  | First carrier scan timestamp   |
| `Delivered`                           | `delivered`                         | VARCHAR(50)  | Delivery timestamp             |
| `Delivered Days`                      | `delivered_days`                    | VARCHAR(50)  | Days to delivery               |
| `Delivered Business Days`             | `delivered_business_days`           | VARCHAR(50)  | Business days to delivery      |
| `Zone`                                | `zone`                              | VARCHAR(50)  | Shipping zone                  |
| `Charged`                             | `charged`                           | VARCHAR(50)  | Base carrier charge            |
| `Store Markup`                        | `store_markup`                      | VARCHAR(50)  | Platform markup                |
| `Platform Charged (With Corrections)` | `platform_charged_with_corrections` | VARCHAR(50)  | Final billed amount            |
| `Commercial`                          | `commercial`                        | VARCHAR(50)  | Commercial pricing             |
| `Order Reference`                     | `order_reference`                   | VARCHAR(255) | Order reference number         |
| `Order Date`                          | `order_date`                        | VARCHAR(50)  | Order date                     |
