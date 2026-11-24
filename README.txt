TELECOM REVENUE ASSURANCE - SYNTHETIC SOURCE DATA
=================================================

Files:
- customers.csv
- usage.csv
- billing.csv
- payments.csv
- subscriptions.csv
- network_events.csv

Business context:
- Usage and billing are reconciled at Customer ID + Billing Period.
- Usage can have multiple transactions per customer per period.
- Billing is generally one bill per customer per period.
- Reconciliation status:
  YY = business key exists in both sources
  YN = business key exists only in usage (Source A)
  NY = business key exists only in billing (Source B)

Intentional data-quality test cases:
- Exact duplicate rows in usage.csv and billing.csv
- NULL customer_id in usage.csv
- Empty customer_id in billing.csv
- Invalid usage_date in usage.csv
- Invalid usage_amount in usage.csv
- Payment with a bill_id that does not exist in billing.csv
- Usage and billing amount mismatches for some matched customer-periods
- Some usage-only and billing-only customer-period combinations

Important:
- All records are synthetic and created for demonstration.
- Do not treat the sample data as real telecom or customer information.
- Keep raw source files unchanged in the Bronze layer.
