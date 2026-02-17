# Eliteworks CSV to Delta Table Column Mapping

## Overview
The Eliteworks CSV file has column names with special characters (spaces, parentheses, slashes) that are not SQL-compliant. The `delta_eliteworks_bill` table uses sanitized snake_case column names following SQL naming conventions (similar to USPS EasyPost pattern).

## ADF Copy Activity Configuration

**Important**: In ADF Copy Activity, you must configure column mapping to transform CSV headers to sanitized column names.

### Configuration Steps:
1. In ADF Copy Activity, go to **Mapping** tab
2. Select **Import schemas** to detect CSV columns
3. Apply the following column mappings:

## Column Mapping Reference

| CSV Column Name (Source) | Delta Table Column (Target) | Data Type | Notes |
|--------------------------|----------------------------|-----------|-------|
| `Time (UTC)` | `time_utc` | VARCHAR(50) | Timestamp of shipment creation |
| `ID` | `shipment_id` | VARCHAR(255) | Unique shipment identifier |
| `USER` | `user_account` | VARCHAR(255) | Account/user name |
| `Tracking` | `tracking_number` | VARCHAR(255) | Carrier tracking number |
| `Status` | `status` | VARCHAR(50) | Shipment status |
| `Carrier` | `carrier` | VARCHAR(50) | Carrier name (USPS) |
| `Service` | `service` | VARCHAR(255) | Service method |
| `Reference` | `reference` | VARCHAR(255) | Reference number |
| `Shipment Weight (Oz)` | `shipment_weight_oz` | VARCHAR(50) | Weight in ounces |
| `Shipment Dryice Weight (Oz)` | `shipment_dryice_weight_oz` | VARCHAR(50) | Dry ice weight |
| `Package Type` | `package_type` | VARCHAR(50) | Package type |
| `Package Length (In)` | `package_length_in` | VARCHAR(50) | Length in inches |
| `Package Width (In)` | `package_width_in` | VARCHAR(50) | Width in inches |
| `Package Height (In)` | `package_height_in` | VARCHAR(50) | Height in inches |
| `From Name` | `from_name` | VARCHAR(255) | Sender name |
| `From Company` | `from_company` | VARCHAR(255) | Sender company |
| `From Street` | `from_street` | VARCHAR(255) | Sender street address |
| `From Apt/Suite` | `from_apt_suite` | VARCHAR(50) | Sender apt/suite |
| `From City` | `from_city` | VARCHAR(100) | Sender city |
| `From State` | `from_state` | VARCHAR(50) | Sender state |
| `From Postal` | `from_postal` | VARCHAR(50) | Sender postal code |
| `From Country` | `from_country` | VARCHAR(10) | Sender country |
| `To Name` | `to_name` | VARCHAR(255) | Recipient name |
| `To Company` | `to_company` | VARCHAR(255) | Recipient company |
| `To Street` | `to_street` | VARCHAR(255) | Recipient street address |
| `To Apt/Suite` | `to_apt_suite` | VARCHAR(50) | Recipient apt/suite |
| `To City` | `to_city` | VARCHAR(100) | Recipient city |
| `To State` | `to_state` | VARCHAR(50) | Recipient state |
| `To Postal` | `to_postal` | VARCHAR(50) | Recipient postal code |
| `To Country` | `to_country` | VARCHAR(10) | Recipient country |
| `First Scan` | `first_scan` | VARCHAR(50) | First carrier scan timestamp |
| `Delivered` | `delivered` | VARCHAR(50) | Delivery timestamp |
| `Delivered Days` | `delivered_days` | VARCHAR(50) | Days to delivery |
| `Delivered Business Days` | `delivered_business_days` | VARCHAR(50) | Business days to delivery |
| `Zone` | `zone` | VARCHAR(50) | Shipping zone |
| `Charged` | `charged` | VARCHAR(50) | Base carrier charge |
| `Store Markup` | `store_markup` | VARCHAR(50) | Platform markup |
| `Platform Charged (With Corrections)` | `platform_charged_with_corrections` | VARCHAR(50) | Final billed amount |
| `Commercial` | `commercial` | VARCHAR(50) | Commercial pricing |
| `Order Reference` | `order_reference` | VARCHAR(255) | Order reference number |
| `Order Date` | `order_date` | VARCHAR(50) | Order date |

## SQL Script References

All transformation scripts (`Insert_ELT_&_CB.sql`, `Sync_Reference_Data.sql`, `Insert_Unified_tables.sql`) reference the **sanitized column names** (right column in table above).

### Example SQL References:
```sql
-- Correct (sanitized names)
SELECT 
    time_utc,
    tracking_number,
    platform_charged_with_corrections
FROM billing.delta_eliteworks_bill;

-- Incorrect (CSV names - will fail)
SELECT 
    [Time (UTC)],
    [Tracking],
    [Platform Charged (With Corrections)]
FROM billing.delta_eliteworks_bill;
```

## Naming Convention Rules Applied

1. **Lowercase with underscores**: All column names use snake_case (e.g., `time_utc`, `tracking_number`)
2. **Remove special characters**: Removed spaces, parentheses, slashes (e.g., `Time (UTC)` → `time_utc`)
3. **Descriptive names**: Expanded abbreviations where helpful (e.g., `ID` → `shipment_id`, `USER` → `user_account`)
4. **Unit suffixes**: Kept unit indicators in name (e.g., `_oz` for ounces, `_in` for inches)
5. **Consistent prefixes**: Used `from_` and `to_` prefixes for address fields

## Benefits of Sanitized Names

✅ **SQL Standard Compliance**: No need for bracket notation `[Column Name]`  
✅ **Easier Querying**: Simpler to write and read queries  
✅ **IDE Support**: Better auto-completion in SQL editors  
✅ **Consistency**: Matches USPS EasyPost pattern  
✅ **Maintainability**: Clearer intent with descriptive names  

## Testing the Mapping

After configuring ADF Copy Activity, test with a sample file:

```sql
-- Verify column mapping worked
SELECT TOP 10 * FROM billing.delta_eliteworks_bill;

-- Check key columns
SELECT 
    time_utc,
    user_account,
    tracking_number,
    service,
    charged,
    store_markup,
    platform_charged_with_corrections
FROM billing.delta_eliteworks_bill;

-- Validate data types can be cast
SELECT 
    CAST(time_utc AS DATETIME) AS time_utc_converted,
    CAST(charged AS DECIMAL(18,2)) AS charged_converted,
    CAST(platform_charged_with_corrections AS DECIMAL(18,2)) AS platform_charged_converted
FROM billing.delta_eliteworks_bill
WHERE tracking_number IS NOT NULL;
```

## Troubleshooting

**Issue**: Column mapping error in ADF  
**Solution**: Ensure "First row as header" is enabled in CSV dataset settings

**Issue**: NULL values in all columns  
**Solution**: Verify column mapping order matches CSV column order exactly

**Issue**: Data truncation warnings  
**Solution**: Check VARCHAR lengths in delta table match expected data sizes

---

*This mapping ensures clean, maintainable SQL code throughout the transformation pipeline.*
