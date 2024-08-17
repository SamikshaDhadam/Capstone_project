select
  customer_id,
  sum(amount) as total_spent,
  count(transaction_id) as total_transactions,
  max(transaction_date) as last_transaction_date
from
  {{ ref('transactions') }}
group by
  customer_id