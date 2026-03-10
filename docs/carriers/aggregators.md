# Aggregator Carriers

**Document Version:** 1.1
**Last Updated:** March 8, 2026
**Status:** Active

---

## Overview

### Business Context

3PL warehouses send shipments using multiple shipping aggregators such as:
- **Eliteworks**
- **EasyPost**
- **FlavorCloud**
- Other aggregator platforms

Each of these aggregators operates as a middleware layer that routes shipments through multiple underlying carriers (FedEx, UPS, USPS, DHL, etc.) based on rate optimization, service level requirements, or other business rules.

### Current Limitation

**The system currently treats Aggregator = Carrier + Shipping Method as a single entity.**

This approach creates the following challenges:

1. **Lack of Granular Visibility:**  
   3PL administrators cannot distinguish between:
   - Shipments sent via **Eliteworks → FedEx Ground**
   - Shipments sent via **EasyPost → FedEx Ground**

2. **Markup Configuration Constraints:**  
   3PLs need to configure different markup rules for the same carrier+method combination depending on the aggregator being used (e.g., different pricing agreements with each aggregator).

3. **Invoicing Requirement:**  
   While 3PL admins need full visibility into Aggregator + Carrier + Shipping Method for internal operations and markup configuration, **the final customer-facing invoice must NOT display the aggregator name** — only the actual carrier and shipping method should appear.

4. **Scalability Issue:**  
   As 3PLs integrate more aggregators, the current model cannot scale to support:
   - Multiple aggregators using the same carrier
   - Different markup strategies per aggregator
   - Accurate cost allocation and reporting

---

### Architecture

The solution introduces a **hierarchical carrier model** where aggregators are stored as carriers, and the actual fulfillment carrier is tracked separately in the shipping method.

#### Carrier Table

The Carrier table stores both aggregators and traditional carriers, with a flag to distinguish between them.

| Carrier ID | Carrier Name | Is Active | Is Aggregator |
|------------|--------------|-----------|---------------|
| 1          | FedEx        | Yes       | No            |
| 2          | UPS          | Yes       | No            |
| 3          | USPS         | Yes       | No            |
| 4          | DHL          | Yes       | No            |
| 5          | Eliteworks   | Yes       | **Yes**       |
| 6          | Passport     | Yes       | No            |
| 7          | EasyPost     | Yes       | **Yes**       |
| 8          | FlavorCloud  | Yes       | **Yes**       |

**Auto-Discovery:** Aggregator billing data may reference carrier names not yet in this table (e.g., a new last-mile carrier). Each aggregator's `Sync_Reference_Data.sql` runs a Block 0 INSERT-IF-NOT-EXISTS that discovers unknown `integrated_carrier` values from billing data and inserts them with `is_aggregator = 0`, `is_active = 1` before the shipping method sync executes. This ensures the downstream `integrated_carrier_id` FK resolves correctly without manual seeding.

#### Shipping Method Table

A field **Integrated Carrier** tracks the actual carrier when shipments are processed through an aggregator.

| Shipping Method ID | Carrier       | Shipping Method | Integrated Carrier | Displayed As (UI)            |
|--------------------|---------------|-----------------|--------------------|-----------------------------|
| 101                | FedEx         | Ground          | *(empty)*          | FedEx Ground                 |
| 102                | Eliteworks    | Ground          | FedEx              | Eliteworks (FedEx) Ground    |
| 103                | EasyPost      | Ground          | FedEx              | EasyPost (FedEx) Ground      |
| 105                | Eliteworks    | 2-Day           | UPS                | Eliteworks (UPS) 2-Day       |
| 106                | FedEx         | Express Saver   | *(empty)*          | FedEx Express Saver          |

**Key Fields:**
- **Carrier:** The aggregator (if used) or the direct carrier
- **Integrated Carrier:** The actual fulfillment carrier when using an aggregator (blank for direct shipments)
- **Shipping Method:** The service level (Ground, 2-Day, Express, etc.)

---

### Markup Configuration

**Display Logic:**
- If **Integrated Carrier** is present: Show as `"Aggregator (Carrier)"`
  - Example: `"Eliteworks (FedEx)"`
- If **Integrated Carrier** is blank: Show carrier name only
  - Example: `"FedEx"`

| Carrier                  | Shipping Method | Markup % |
|--------------------------|-----------------|----------|
| FedEx                    | Ground          | 15%      |
| Eliteworks (FedEx)       | Ground          | 18%      |
| EasyPost (FedEx)         | Ground          | 17%      |
| Eliteworks (UPS)         | 2-Day           | 22%      |

---

### Invoice Display Logic

Customer-facing invoices must **NOT** display the aggregator name — only the actual fulfillment carrier.

- If **Integrated Carrier** is populated: Show the Integrated Carrier
- If **Integrated Carrier** is blank: Show the Carrier

---

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         3PL Admin Interface                             │
├─────────────────────────────────────────────────────────────────────────┤
│  Carrier Selection: [Eliteworks (FedEx) ▼]                            │
│  Shipping Method:   [Ground ▼]                                         │
│  Markup:            [18%]                                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ System Stores:
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Shipping Method Configuration                       │
├─────────────────────────────────────────────────────────────────────────┤
│  Carrier:               Eliteworks                                      │
│  Shipping Method:       Ground                                          │
│  Integrated Carrier:    FedEx                                           │
│  Markup:                18%                                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Customer Invoice Displays:
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Customer Invoice                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  Carrier:         FedEx  ← (Shows Integrated Carrier, not Aggregator)  │
│  Method:          Ground                                                │
│  Amount:          $12.50 (includes 18% Eliteworks markup)              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### Implementation Phases

#### Phase 1: Schema Update
- Add `integrated_carrier_id` column to `dbo.shipping_method`
- Add `is_aggregator` flag to `dbo.carrier` (optional, for filtering)
- Create foreign key constraints
- Backfill existing data (set `integrated_carrier_id = NULL` for direct carrier shipments)

#### Phase 2: Data Migration
- Identify existing aggregator records (Eliteworks, EasyPost, FlavorCloud)
- Split aggregator shipping methods into proper hierarchy

#### Phase 3: UI Updates
- Update markup configuration UI to display concatenated carrier names
- Update shipping method selection dropdowns
- Add filtering options for "Aggregator Shipments" vs "Direct Carrier Shipments"

#### Phase 4: Invoice Generation
- Update invoice generation logic to use `COALESCE(integrated_carrier_id, carrier_id)`
- Verify customer-facing invoices hide aggregator names

#### Phase 5: Testing & Validation
- Test markup calculations for aggregator vs direct carrier
- Validate invoice display logic
- Test edge cases (aggregator shipping via another aggregator)

---

### Edge Cases

1. **Nested Aggregators:** Only track one level deep (`integrated_carrier_id` always points to the final carrier).
2. **Aggregator-Direct Hybrid:** Fully supported — separate `shipping_method` records for direct vs aggregator-routed.
3. **Reporting:** Filter by Carrier field where Integrated Carrier is populated for aggregator reports; group by Integrated Carrier for actual carrier reports.

---

### Benefits Summary

| Stakeholder | Benefit |
|-------------|---------|
| **3PL Admins** | Full visibility into Aggregator + Carrier + Method combinations |
| **Finance Team** | Different markup rates per aggregator |
| **End Customers** | Clean invoices showing only the actual carrier |
| **System Scalability** | Unlimited aggregator integrations without schema changes |
| **Reporting & Analytics** | Accurate cost allocation and aggregator comparison |

---

---

## EasyPost (USPS)

### Overview
USPS EasyPost provides domestic shipping through the EasyPost platform. Data is exported as a 33-column CSV with one row per shipment.

### Invoice Information
- **Invoice Number**: Generated as `{carrier_account_id}-{yyyy-MM-dd}` (deterministic, computed in SQL)
- **Invoice Date**: Derived from `created_at` column
- **Account Number**: `carrier_account_id` column (column 33)

### Charge Structure
**Format:** Narrow (5 distinct charge types, static — seeded once during setup)

| Charge Name | Description | Freight Flag |
|-------------|-------------|--------------|
| **Base Rate** | Primary shipping charge | freight=1 |
| **Label Fee** | Label generation fee | freight=0 |
| **Unknown Charges** | Calculated as `postage_fee - rate` | freight=0 |
| **Carbon Offset Fee** | Optional environmental fee | freight=0 |
| **Insurance Fee** | Optional insurance coverage | freight=0 |

**Unpivoting:** CROSS APPLY in Insert_Unified_tables.sql  
**Category:** All charges → `'Other'` (charge_category_id = 11)

### Unit Conversions
- **Weight:** Already in OZ (no conversion needed)
- **Dimensions:** Already in IN (no conversion needed)

### Schema
- **Delta table:** `billing.delta_easypost_bill` (33 columns, all VARCHAR)
- **Normalized table:** `billing.easypost_bill` (18 columns, typed)

### Key Differences from Direct Carriers

| Aspect | Direct Carriers (FedEx) | EasyPost |
|--------|------------------------|----------|
| Format | Wide (50+ charge columns) | Narrow (5 charge columns) |
| Unpivoting | View (vw_FedExCharges) | CROSS APPLY in script |
| Weight Unit | LB → OZ conversion | Already OZ |
| Invoice Number | From CSV | Computed from carrier_account_id + date |
| Charge Types | 50+ dynamic charges | 5 fixed charges |

---

## Eliteworks

### Overview
Eliteworks provides shipment tracking and billing data through their platform. Data is exported as a CSV with one row per shipment, containing costs, dimensions, and tracking information.

### Invoice Information
- **Invoice Number**: Generated as `Eliteworks_YYYY-MM-DD` based on the latest shipment timestamp
- **Invoice Date**: `CAST(MAX(time_utc) AS DATE)`
- **Account Number**: Taken from the `USER` column (e.g., "Falcon IT")
- **Invoice Grouping**: All shipments in a file grouped under a single invoice

### Charge Structure
**Single charge per shipment:**

| Charge Name | Description |
|-------------|-------------|
| **Platform Charged (With Corrections)** | Authoritative billed amount (Base Rate + Store Markup with corrections) |

Supporting fields (`Charged`, `Store Markup`) are preserved in `eliteworks_bill` for audit only — NOT stored in `shipment_charges` to avoid double-counting.

### Unit Conversions
- **Weight:** Already in OZ (no conversion needed)
- **Dimensions:** Already in IN (no conversion needed)

### CSV Column Mapping

| CSV Column Name | Delta Table Column | Notes |
|----------------|-------------------|-------|
| Time (UTC) | time_utc | Timestamp of shipment creation |
| ID | shipment_id | Unique shipment identifier |
| USER | user_account | Account/user name |
| Tracking | tracking_number | Carrier tracking number |
| Status | status | Shipment status |
| Carrier | carrier | Carrier name (typically USPS) |
| Service | service | Service method |
| Reference | reference | Reference number |
| Shipment Weight (Oz) | shipment_weight_oz | Weight in ounces |
| Package Length (In) | package_length_in | Length in inches |
| Package Width (In) | package_width_in | Width in inches |
| Package Height (In) | package_height_in | Height in inches |
| Zone | zone | Shipping zone |
| Charged | charged | Base carrier charge (audit) |
| Store Markup | store_markup | Platform markup (audit) |
| Platform Charged (With Corrections) | platform_charged_with_corrections | Final billed amount |

*(Address fields and additional columns omitted for brevity — see full mapping in eliteworks_example_bill.csv)*

### Key Business Rules
1. All shipments in a file grouped under one invoice based on latest shipment date
2. Only Platform Charged (with corrections) stored as the authoritative charge
3. Base Rate and Store Markup are audit fields only
4. New shipping methods auto-discovered from data

---

## FlavorCloud

### Overview
FlavorCloud provides international shipping and logistics services with detailed billing including duties, taxes, landed costs, and carrier charges. Data is a CSV with one row per shipment.

### Invoice Information
- **Invoice Number**: Unique identifier (e.g., "FLCL-1jfpthha9r78")
- **Date**: "Mon dd, yyyy" format (e.g., "Jan 25, 2026") — parsed with SQL CONVERT style 107
- **Account**: Origin Location field (e.g., "Falcon Fulfillment UT")
- **Shipment Date**: "mm-dd-yyyy" format — parsed with SQL CONVERT style 110

### Special CSV Features
- **Total Row**: CSV contains a "Total" row at the end (filtered out during processing)
- **Summary Rows**: May contain summary distribution rows without shipment numbers (filtered out)

### Charge Structure (4 Charge Types)

| Charge Name | Description | Freight Flag |
|-------------|-------------|--------------|
| **Shipping Charges (USD)** | Carrier transportation cost | freight=1 |
| **Commissions (USD)** | FlavorCloud commission | freight=0 |
| **LandedCost (Duty + Taxes + Fees) (USD)** | Combined import duties, taxes, and fees | freight=0 |
| **Insurance (USD)** | Shipment insurance | freight=0 |

**Category:** All charges → `'Other'` (charge_category_id = 11)

**Validation:**
```
SUM(Shipping Charges + Commissions + LandedCost + Insurance) = Shipment Total Charges
```

### Unit Conversions
- **Weight:** LB → OZ (LB × 16)
- **Dimensions:** Typically already in IN (no conversion needed)
- **Fallback:** KG × 35.274 for weight; CM ÷ 2.54, MM ÷ 25.4 for dimensions

### International Shipping Context
- **Terms of Trade:** Most shipments use DDP (Delivered Duty Paid)
- **Landed Cost Components:** Duties + Taxes (VAT/GST) + Fees (customs/brokerage)
- **Common Destinations:** EU (IT, DE, FR), Switzerland, Norway, Australia
- **Multi-Carrier:** FlavorCloud routes through UPS, APC LAX, and others

### Key Business Rules
1. International shipments with landed cost calculations
2. DDP standard — seller pays all duties, taxes, and fees
3. Zero-amount charges excluded from shipment_charges
4. LandedCost stored as a single charge (not split into Duties/Taxes/Fees)
5. New service levels auto-discovered from data

---

## Notes for Business Users

- **Aggregators are not carriers**: They are middleware platforms that route shipments through actual carriers
- **Markup transparency**: Each aggregator can have different markup rates for the same carrier+method
- **Invoice privacy**: Customer invoices show only the actual carrier, never the aggregator name
- **Corrections**: Platform Charged and Shipment Total Charges already include corrections/adjustments
- **Unit consistency**: All weights standardized to OZ, all dimensions to IN across all aggregators
