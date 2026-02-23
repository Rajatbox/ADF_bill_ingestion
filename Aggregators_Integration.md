# Aggregators Integration - Business Document

**Document Version:** 1.0  
**Last Updated:** February 21, 2026  
**Status:** Proposed

---

## Problem Statement

### Business Context

3PL warehouses send shipments using multiple shipping aggregators such as:
- **Eliteworks**
- **Passport**
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
   - Shipments sent via **Passport → FedEx Ground**
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

## Proposed Solution

### Architecture Overview

The solution introduces a **hierarchical carrier model** where aggregators are stored as carriers, and the actual fulfillment carrier is tracked separately in the shipping method.

### Data Model Changes

#### 1. Carrier Table

The Carrier table will store both aggregators and traditional carriers, with a flag to distinguish between them.

**Example Data:**

| Carrier ID | Carrier Name | Carrier Code | Is Active | Is Aggregator |
|------------|--------------|--------------|-----------|---------------|
| 1          | FedEx        | FEDEX        | Yes       | No            |
| 2          | UPS          | UPS          | Yes       | No            |
| 3          | USPS         | USPS         | Yes       | No            |
| 4          | DHL          | DHL          | Yes       | No            |
| 5          | Eliteworks   | ELITEWORKS   | Yes       | **Yes**       |
| 6          | Passport     | PASSPORT     | Yes       | **Yes**       |
| 7          | EasyPost     | EASYPOST     | Yes       | **Yes**       |

---

#### 2. Shipping Method Table (NEW COLUMN ADDED)

A new field **Integrated Carrier** will be added to track the actual carrier when shipments are processed through an aggregator.

**Example Data:**

| Shipping Method ID | Carrier       | Shipping Method | Integrated Carrier | Displayed As (UI)            |
|--------------------|---------------|-----------------|--------------------|-----------------------------|
| 101                | FedEx         | Ground          | *(empty)*          | FedEx Ground                 |
| 102                | Eliteworks    | Ground          | FedEx              | Eliteworks (FedEx) Ground    |
| 103                | Passport      | Ground          | FedEx              | Passport (FedEx) Ground      |
| 104                | EasyPost      | Ground          | FedEx              | EasyPost (FedEx) Ground      |
| 105                | Eliteworks    | 2-Day           | UPS                | Eliteworks (UPS) 2-Day       |
| 106                | FedEx         | Express Saver   | *(empty)*          | FedEx Express Saver          |

**Key Fields:**
- **Carrier:** The aggregator (if used) or the direct carrier
- **Integrated Carrier:** The actual fulfillment carrier when using an aggregator (blank for direct shipments)
- **Shipping Method:** The service level (Ground, 2-Day, Express, etc.)

---

### Implementation Details

#### 3. Markup Configuration UI

**Current Constraint:** The markup configuration table is limited to 3 columns.

**Solution:** Concatenate aggregator and carrier names in the **Carrier** column for display purposes only.

**Display Logic:**
- If **Integrated Carrier** is present: Show as `"Aggregator (Carrier)"`
  - Example: `"Eliteworks (FedEx)"`
- If **Integrated Carrier** is blank: Show carrier name only
  - Example: `"FedEx"`

**Markup Configuration Example:**

| Carrier                  | Shipping Method | Markup % |
|--------------------------|-----------------|----------|
| FedEx                    | Ground          | 15%      |
| Eliteworks (FedEx)       | Ground          | 18%      |
| Passport (FedEx)         | Ground          | 20%      |
| EasyPost (FedEx)         | Ground          | 17%      |
| Eliteworks (UPS)         | 2-Day           | 22%      |

**Benefits:**
- 3PL can configure **different markup rates** for the same carrier+method when accessed via different aggregators
- Clear visibility into aggregator vs. direct carrier shipments
- Supports complex pricing strategies per aggregator relationship

---

#### 4. Invoice Generation Logic

**Requirement:** Customer-facing invoices must **NOT** display the aggregator name — only the actual fulfillment carrier.

**Solution:** The system will prioritize showing the **Integrated Carrier** (if present) or fall back to the **Carrier** field for invoice display.

**Invoice Display Logic:**
- If **Integrated Carrier** is populated: Show the Integrated Carrier
  - Example: Show `"FedEx"` instead of `"Eliteworks"`
- If **Integrated Carrier** is blank: Show the Carrier
  - Example: Show `"FedEx"`

**Customer Invoice Example:**

| Tracking Number       | Carrier   | Shipping Method | Amount |
|-----------------------|-----------|-----------------|--------|
| 1Z999AA10123456784    | **FedEx** | Ground          | $12.50 |
| 1Z999AA10123456785    | **FedEx** | Ground          | $14.25 |

*(Both shipments may have been sent via different aggregators internally, but the invoice only shows "FedEx")*

---

### Data Flow Diagram

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

## Benefits Summary

| Stakeholder | Benefit |
|-------------|---------|
| **3PL Admins** | Full visibility into Aggregator + Carrier + Method combinations for accurate cost tracking and markup configuration |
| **Finance Team** | Ability to set different markup rates per aggregator, supporting multiple pricing agreements |
| **End Customers** | Clean invoices showing only the actual carrier (no aggregator names exposed) |
| **System Scalability** | Supports unlimited aggregator integrations without schema changes |
| **Reporting & Analytics** | Accurate cost allocation, carrier performance analysis, and aggregator comparison reports |

---

## Implementation Phases

### Phase 1: Schema Update
- Add `integrated_carrier_id` column to `dbo.shipping_method`
- Add `is_aggregator` flag to `dbo.carrier` (optional, for filtering)
- Create foreign key constraints
- Backfill existing data (set `integrated_carrier_id = NULL` for direct carrier shipments)

### Phase 2: Data Migration
- Identify existing aggregator records (Eliteworks, Passport, EasyPost, FlavorCloud)
- Split aggregator shipping methods into proper hierarchy:
  - `carrier_id` → Aggregator
  - `integrated_carrier_id` → Actual carrier

### Phase 3: UI Updates
- Update markup configuration UI to display concatenated carrier names
- Update shipping method selection dropdowns
- Add filtering options for "Aggregator Shipments" vs "Direct Carrier Shipments"

### Phase 4: Invoice Generation
- Update invoice generation logic to use `COALESCE(integrated_carrier_id, carrier_id)`
- Verify customer-facing invoices hide aggregator names
- Update invoice templates and reports

### Phase 5: Testing & Validation
- Test markup calculations for aggregator vs direct carrier
- Validate invoice display logic
- Test edge cases (aggregator shipping via another aggregator)

---

## Edge Cases & Considerations

### 1. Nested Aggregators
**Scenario:** Aggregator A uses Aggregator B, which uses Carrier C.

**Solution:** Only track one level deep (`integrated_carrier_id` always points to the final carrier, not intermediate aggregators).

### 2. Aggregator-Direct Hybrid
**Scenario:** 3PL uses both direct FedEx account AND FedEx via Eliteworks.

**Solution:** This is the primary use case — the proposed schema fully supports this with separate `shipping_method` records.

### 3. Reporting Requirements
**Scenario:** Finance needs to see aggregator performance vs direct carrier performance.

**Solution:** The system will support two types of reports:
- **Aggregator Performance Reports:** Filter by Carrier field where Integrated Carrier is populated
- **Actual Carrier Performance Reports:** Group by Integrated Carrier field (showing FedEx, UPS, etc. regardless of aggregator used)

**Example Report - Aggregator Performance:**

| Aggregator   | Shipment Count | Total Spend |
|--------------|----------------|-------------|
| Eliteworks   | 1,250          | $18,450.00  |
| Passport     | 850            | $14,200.00  |
| EasyPost     | 620            | $9,875.00   |
| Direct (FedEx)| 400           | $5,600.00   |

---

## Comments

**Open Questions:**

*(To be added during stakeholder review)*

**Risks:**
- Reporting tools/dashboards may need schema adjustments
- Training required for 3PL admins to understand Aggregator + Carrier model

---

**End of Document**

