select 
transaction_id,
    customer_id,
    amount,
    transaction_date,
    currency,
    transaction_type,
    channel,
    merchant_name,
    merchant_category,
    location_country,
    location_city,
    is_flagged,
    from GLOBALBANK.raw_data.transacation_raw