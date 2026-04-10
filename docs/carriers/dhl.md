# DHL Billing Data Specification

## File Structure

- Row 1 is the header record (HDR) -- not a shipment row
  - B1 = bill date, C1 = invoice number, D1 = account number
  - G1:K1 = warehouse address (verify account/warehouse match)
  - L1 = total bill amount, N1 = number of shipments
- No column-name header row; data starts at row 2
- Each shipment produces two rows; the second row adds no value (skip it)
- Both domestic and international shipments can appear in the same file

## Tracking Number Logic

- **International**: Column 12 (`customer_confirm`) used as-is
- **Domestic**: `'420' + LEFT(zip, 5) + Column 13 (delivery_confirm)`
- **Overlabel**: `'420' + LEFT(zip, 5) + Column 67 (overlabeled_value)` — alternate tracking used by WMS for last-mile
- **Resolution**: overlabel wins when present; else `recipient_country = 'US'` → domestic; otherwise → international

## Two-Layer Naming Convention

| Layer | Table | Naming Rule |
|-------|-------|-------------|
| Bronze | `billing.delta_dhl_bill` | DHL official field names in snake_case -- mirrors source exactly |
| Silver | `billing.dhl_bill` | Our custom/friendly column names |

The `Insert_ELT_&_CB.sql` script maps bronze names to silver names.

## Total Validation

`total_amount = SUM of all 38 charge columns` (see charge columns marked below).  
Must equal Cell L1 from the HDR row.

## Column Mapping (79 columns)

| Col # | Excel | DHL Name (delta_dhl_bill)    | Old delta column                           | dhl_bill column (silver)                  |
| ----- | ----- | ---------------------------- | ------------------------------------------ | ----------------------------------------- |
| 1     | A     | record_type_2                | Prop_1                                     | -                                         |
| 2     | B     | sold_to                      | account_number                             | - (goes to carrier_bill.account_number)   |
| 3     | C     | inventory_positioner         | Prop_3                                     | -                                         |
| 4     | D     | bol_number                   | Prop_4                                     | -                                         |
| 5     | E     | Prop_5                       | Prop_5                                     | -                                         |
| 6     | F     | billing_ref1                 | Prop_6                                     | -                                         |
| 7     | G     | processing_facility          | Prop_7                                     | -                                         |
| 8     | H     | pickup_from                  | Prop_8                                     | -                                         |
| 9     | I     | pickup_date                  | shipping_date                              | shipping_date                             |
| 10    | J     | pickup_time                  | Prop_10                                    | -                                         |
| 11    | K     | internal_tracking            | Prop_11                                    | -                                         |
| 12    | L     | customer_confirm             | international_tracking_number              | international_tracking_number             |
| 13    | M     | delivery_confirm             | domestic_tracking_number                   | domestic_tracking_number (420+zip5+col13) |
| 14    | N     | Prop_14                      | Prop_14                                    | -                                         |
| 15    | O     | recipient_address1           | recipient_address_line_1                   | -                                         |
| 16    | P     | recipient_address2           | recipient_address_line_2                   | -                                         |
| 17    | Q     | recipient_city               | recipient_city                             | -                                         |
| 18    | R     | recipient_state              | recipient_state_province                   | -                                         |
| 19    | S     | recipient_zip                | recipient_zip_postal_code                  | recipient_zip_postal_code                 |
| 20    | T     | recipient_country            | recipient_country                          | recipient_country                         |
| 21    | U     | material_or_vas_num          | Prop_21                                    | -                                         |
| 22    | V     | material_or_vas_desc         | shipping_method                            | shipping_method                           |
| 23    | W     | actual_weight                | shipped_weight                             | shipped_weight                            |
| 24    | X     | uom_actual_weight            | shipped_weight_unit_of_measure             | shipped_weight_unit                       |
| 25    | Y     | billing_weight               | billed_weight                              | billed_weight                             |
| 26    | Z     | uom_billing_weight           | billed_weight_unit_of_measure              | billed_weight_unit                        |
| 27    | AA    | quantity                     | Prop_27                                    | -                                         |
| 28    | AB    | uom_quantity                 | Prop_28                                    | -                                         |
| 29    | AC    | pricing_zone                 | [zone]                                     | [zone]                                    |
| 30    | AD    | charge                       | transportation_cost                        | transportation_cost                       |
| 31    | AE    | billing_ref2_or_cust_ref1    | Prop_31                                    | -                                         |
| 32    | AF    | cust_reference2              | Prop_32                                    | -                                         |
| 33    | AG    | workshare_dropoff            | Prop_33                                    | workshare_dropoff                         |
| 34    | AH    | workshare_sort               | Prop_34                                    | workshare_sort                            |
| 35    | AI    | workshare_stamp              | Prop_35                                    | workshare_stamp                           |
| 36    | AJ    | workshare_machine            | Prop_36                                    | workshare_machine                         |
| 37    | AK    | workshare_manifest           | Prop_37                                    | workshare_manifest                        |
| 38    | AL    | workshare_bpm                | Prop_38                                    | workshare_bpm                             |
| 39    | AM    | workshare_future_use_1       | Prop_39                                    | - (reserved for future use)               |
| 40    | AN    | workshare_future_use_2       | Prop_40                                    | - (reserved for future use)               |
| 41    | AO    | workshare_future_use_3       | Prop_41                                    | - (reserved for future use)               |
| 42    | AP    | surcharge_content_endorse    | Prop_42                                    | surcharge_content_endorse                 |
| 43    | AQ    | surcharge_unassignable_add   | Prop_43                                    | surcharge_unassignable_add                |
| 44    | AR    | surcharge_special_handling   | Prop_44                                    | surcharge_special_handling                |
| 45    | AS    | surcharge_late_arrival       | Prop_45                                    | surcharge_late_arrival                    |
| 46    | AT    | surcharge_usps_qualif        | Prop_46                                    | surcharge_usps_qualif                     |
| 47    | AU    | surcharge_client_srd         | Prop_47                                    | surcharge_client_srd                      |
| 48    | AV    | surcharge_nqd                | non_qualified_dimensional_charges          | non_qualified_dimensional_charges         |
| 49    | AW    | returned_mail_unassignable   | Prop_49                                    | returned_mail_unassignable                |
| 50    | AX    | returned_mail_unprocessable  | Prop_50                                    | returned_mail_unprocessable               |
| 51    | AY    | returned_mail_recall         | Prop_51                                    | returned_mail_recall                      |
| 52    | AZ    | returned_mail_duplicate      | Prop_52                                    | returned_mail_duplicate                   |
| 53    | BA    | returned_mail_cont_assur     | Prop_53                                    | returned_mail_cont_assur                  |
| 54    | BB    | returned_mail_move_update    | Prop_54                                    | returned_mail_move_update                 |
| 55    | BC    | gst_tax                      | Prop_55                                    | gst_tax                                   |
| 56    | BD    | hst_tax                      | Prop_56                                    | hst_tax                                   |
| 57    | BE    | pst_tax                      | Prop_57                                    | pst_tax                                   |
| 58    | BF    | vat_tax                      | Prop_58                                    | vat_tax                                   |
| 59    | BG    | duties                       | Prop_59                                    | duties                                    |
| 60    | BH    | other_tax                    | Prop_60                                    | other_tax                                 |
| 61    | BI    | returned_mail_paper_invoice  | Prop_61                                    | returned_mail_paper_invoice               |
| 62    | BJ    | returned_mail_screening      | Prop_62                                    | returned_mail_screening                   |
| 63    | BK    | returned_mail_non_auto_flats | Prop_63                                    | returned_mail_non_auto_flats              |
| 64    | BL    | xb_customs_surcharge         | Prop_64                                    | xb_customs_surcharge                      |
| 65    | BM    | surcharge_fuel               | fuel_surcharge_amount                      | fuel_surcharge_amount                     |
| 66    | BN    | min_pickup_charge            | Prop_66                                    | min_pickup_charge                         |
| 67    | BO    | overlabeled_value            | overlabel_tracking_number                  | overlabel_tracking_number                 |
| 68    | BP    | dim_weight                   | Prop_68                                    | -                                         |
| 69    | BQ    | uom_dim_weight               | Prop_69                                    | -                                         |
| 70    | BR    | dim_length                   | Prop_70                                    | -                                         |
| 71    | BS    | dim_width                    | Prop_71                                    | -                                         |
| 72    | BT    | dim_height                   | Prop_72                                    | -                                         |
| 73    | BU    | uom_dims                     | Prop_73                                    | -                                         |
| 74    | BV    | peak_surcharge               | Prop_74                                    | peak_surcharge                            |
| 75    | BW    | broker_fee                   | Prop_75                                    | broker_fee                                |
| 76    | BX    | extra_length_surcharge       | Prop_76                                    | extra_length_surcharge                    |
| 77    | BY    | extra_volume_surcharge       | Prop_77                                    | extra_volume_surcharge                    |
| 78    | BZ    | delivery_area_surcharge      | Prop_78                                    | **delivery_area_surcharge_amount**        |
| 79    | CA    | dangerous_goods_charge       | delivery_area_surcharge_amount (WRONG)     | **dangerous_goods_charge**                |

- Col 3 = DHL official name in snake_case -- this becomes the `delta_dhl_bill` column name (bronze)
- Col 4 = what the delta column was called before this change
- Col 5 = the `dhl_bill` column name (silver); `-` means not carried to silver
- **Rows 78/79 are the critical fix** -- DAS was mapped at Col 79 but actually lives at Col 78
