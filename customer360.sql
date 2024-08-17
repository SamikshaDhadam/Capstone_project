select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.phone_number,
  c.gender,
  c.date_of_birth,
  c.city,
  c.country,
  c.occupation,
  c.income_bracket,
  c.customer_since,
  ca.total_accounts,
  ca.total_balance,
  ca.last_account_opened,
  cs.total_spent,
  cs.total_transactions,
  cs.last_transaction_date
from
  {{ ref('customers_base') }} c
left join
  {{ ref('customer_accounts') }} ca
on
  c.customer_id = ca.customer_id
left join
  {{ ref('customer_spending') }} cs
on
  c.customer_id = cs.customer_id
