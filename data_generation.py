import random
from faker import Faker
import pandas as pd
import uuid

faker = Faker()

# 1. Transaction Data Simulation
def generate_transaction_data(customers, accounts, num_records=1000):
    channels = ['online', 'mobile', 'ATM', 'in-branch']
    transaction_types = ['purchase', 'transfer', 'withdrawal', 'deposit']
    currencies = ['USD', 'EUR', 'GBP', 'JPY','INR']
    countries = ['USA', 'UK', 'Germany', 'Japan','IND','CANADA']
    categories = ['retail', 'groceries', 'entertainment', 'utilities', 'travel','Electronics']
    
    data = []
    for _ in range(num_records):
        transaction_id = str(uuid.uuid4())
        customer_id = random.choice(customers)
        #account_id = random.choice(accounts)
        transaction_date = faker.date_time_between(start_date='-2y', end_date='now')
        amount = round(random.uniform(5.0, 2000.0), 2)
        currency = random.choice(currencies)
        transaction_type = random.choice(transaction_types)
        channel = random.choice(channels)
        merchant_name = faker.company()
        merchant_category = random.choice(categories)
        location_country = random.choice(countries)
        location_city = faker.city()
        is_flagged = random.choices([True, False], weights=[5, 95])[0]

        # Inject anomalies and potential fraud patterns
        if random.random() < 0.01:  # 1% chance of fraud
            amount *= 10
            is_flagged = True

        data.append([
            transaction_id, customer_id, transaction_date, amount, currency,
            transaction_type, channel, merchant_name, merchant_category,
            location_country, location_city, is_flagged
        ])
    
    df = pd.DataFrame(data, columns=[
        'transaction_id', 'customer_id', 'transaction_date', 'amount', 'currency',
        'transaction_type', 'channel', 'merchant_name', 'merchant_category',
        'location_country', 'location_city', 'is_flagged'
    ])
    return df

# 2. Customer Data Simulation
def generate_customer_data(num_customers=1000):
    occupations = ['Engineer', 'Doctor', 'Teacher', 'Artist', 'Lawyer']
    income_brackets = ['low', 'middle', 'high']
    
    data = []
    customer_ids = []
    for _ in range(num_customers):
        customer_id = str(uuid.uuid4())
        customer_ids.append(customer_id)
        first_name = faker.first_name()
        last_name = faker.last_name()
        date_of_birth = faker.date_of_birth(minimum_age=18, maximum_age=80)
        gender = random.choice(['Male', 'Female', 'Other'])
        email = faker.email()
        phone_number = faker.phone_number()
        address = faker.address().replace("\n", ", ")
        city = faker.city()
        country = faker.country()
        occupation = random.choice(occupations)
        income_bracket = random.choice(income_brackets)
        customer_since = faker.date_this_decade()

        data.append([
            customer_id, first_name, last_name, date_of_birth, gender, email,
            phone_number, address, city, country, occupation, income_bracket, customer_since
        ])
    
    df = pd.DataFrame(data, columns=[
        'customer_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'email',
        'phone_number', 'address', 'city', 'country', 'occupation', 'income_bracket', 'customer_since'
    ])
    return df, customer_ids

# 3. Account Data Simulation
def generate_account_data(customer_ids, num_accounts=1000):
    account_types = ['checking', 'savings', 'credit card', 'loan']
    account_statuses = ['active', 'dormant', 'closed']
    
    data = []
    account_ids = []
    for _ in range(num_accounts):
        account_id = str(uuid.uuid4())
        account_ids.append(account_id)
        customer_id = random.choice(customer_ids)
        account_type = random.choice(account_types)
        account_status = random.choice(account_statuses)
        open_date = faker.date_this_decade()
        current_balance = round(random.uniform(-1000.0, 50000.0), 2)
        currency = random.choice(['USD', 'EUR', 'GBP', 'JPY'])
        credit_limit = round(random.uniform(1000.0, 50000.0), 2) if account_type == 'credit card' else None

        data.append([
            account_id, customer_id, account_type, account_status, open_date,
            current_balance, currency, credit_limit
        ])
    
    df = pd.DataFrame(data, columns=[
        'account_id', 'customer_id', 'account_type', 'account_status', 'open_date',
        'current_balance', 'currency', 'credit_limit'
    ])
    return df, account_ids

# 4. Credit Bureau Data Simulation
def generate_credit_data(customer_ids):
    data = []
    for customer_id in customer_ids:
        credit_score = random.randint(300, 850)
        number_of_credit_accounts = random.randint(1, 10)
        total_credit_limit = round(random.uniform(1000.0, 100000.0), 2)
        total_credit_used = round(random.uniform(0.0, total_credit_limit), 2)
        number_of_late_payments = random.randint(0, 10)
        bankruptcies = random.randint(0, 2)

        data.append([
            customer_id, credit_score, number_of_credit_accounts, total_credit_limit,
            total_credit_used, number_of_late_payments, bankruptcies
        ])
    
    df = pd.DataFrame(data, columns=[
        'customer_id', 'credit_score', 'number_of_credit_accounts', 'total_credit_limit',
        'total_credit_used', 'number_of_late_payments', 'bankruptcies'
    ])
    return df

# 5. Watchlist Data Simulation
def generate_watchlist_data(merchant_name_list,num_entities=100):
    entity_types = ['individual', 'organization']
    risk_categories = ['low', 'medium', 'high']
    sources = ['government', 'financial institution', 'international organization']
    
    data = []
    for _ in range(num_entities):
        entity_id = str(uuid.uuid4())
        #entity_name = faker.name()
        entity_name = random.choice(merchant_name_list)
        entity_type = random.choice(entity_types)
        risk_category = random.choice(risk_categories)
        listed_date = faker.date_this_decade()
        source = random.choice(sources)

        data.append([
            entity_id, entity_name, entity_type, risk_category, listed_date, source
        ])
    
    df = pd.DataFrame(data, columns=[
        'entity_id', 'entity_name', 'entity_type', 'risk_category', 'listed_date', 'source'
    ])
    return df

# Main function to generate and save all datasets
def main():
    # Generate customer data
    customer_data, customer_ids = generate_customer_data(1000)
    customer_data.to_csv('customer_data.csv', index=False)
    
    # Generate account data with referential integrity
    account_data, account_ids = generate_account_data(customer_ids, 1000)
    account_data.to_csv('account_data.csv', index=False)
    
    # Generate transaction data with referential integrity
    transaction_data = generate_transaction_data(customer_ids, account_ids, 1000)
    transaction_data.to_csv('transaction_data.csv', index=False)

    
    # Generate credit data with referential integrity
    credit_data = generate_credit_data(customer_ids)
    credit_data.to_csv('credit_data.csv', index=False)
    

    # Generate watchlist data

    merchant_name_list = transaction_data['merchant_name'].to_list()
    watchlist_data = generate_watchlist_data(merchant_name_list,100)
    watchlist_data.to_csv('watchlist_data.csv', index=False)
    
    # Analyze transactions to create watchlist entries
    high_risk_transactions = transaction_data[transaction_data['amount'] > 10000]  
    unique_high_risk_entities = high_risk_transactions['merchant_name'].unique()

    medium_risk_transactions = transaction_data[transaction_data['amount']> 5000]
    unique_medium_risk_entities = medium_risk_transactions['merchant_name'].unique()

    low_risk_transactions = transaction_data[(transaction_data['amount'] > 1000) & (transaction_data['amount'] <= 5000)]
    unique_low_risk_entities = low_risk_transactions['merchant_name'].unique()

    # Create watchlist entries
    watchlist_entries_high = pd.DataFrame([{'entity_name': name, 'risk_category': 'High'} for name in unique_high_risk_entities])
    watchlist_entries_medium = pd.DataFrame([{'entity_name': name, 'risk_category': 'Medium'} for name in unique_medium_risk_entities])
    watchlist_entries_low = pd.DataFrame([{'entity_name': name, 'risk_category': 'Low'} for name in unique_low_risk_entities])

    watchlist_entries = pd.concat([watchlist_entries_high, watchlist_entries_medium, watchlist_entries_low])
    watchlist_entries.to_csv('watchlist.csv', index=False)

    print("All datasets have been generated and saved as CSV files.")

if __name__ == "__main__":
    main()
