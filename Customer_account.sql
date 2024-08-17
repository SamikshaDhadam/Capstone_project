select
  customer_id,
  count(account_id) as total_accounts,
  sum(current_balance) as total_balance,
  max(open_date) as last_account_opened
from
  {{ ref('account') }}
group by
  customer_id
