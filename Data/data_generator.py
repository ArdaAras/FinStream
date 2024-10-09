# This file generates random data 

from Data.definitions import customer_ids, transaction_types, account_types, currencies, merchants, transaction_statuses, fake_generator, random

def generate_financial_transaction(num_records: int = 10) -> list:
    """
    Generates a list of random financial transactions and returns list 
    """
    financial_data = []

    for _ in range(num_records):    
        # Randomly generate transaction details
        transaction_id = fake_generator.uuid4()
        customer_id = random.choice(customer_ids)
        transaction_amount = round(random.uniform(-5000, 5000), 2)  # Debits are negative, credits are positive
        transaction_type = random.choice(transaction_types)
        account_type = random.choice(account_types)
        transaction_status = random.choice(transaction_statuses)
        currency = random.choice(currencies)
        merchant = random.choice(merchants)
        transaction_date = fake_generator.date_time_this_year()
        year = transaction_date.year
        month = transaction_date.month
        day = transaction_date.day
        hour = transaction_date.hour
        minutes = transaction_date.minute
        
        # Simulate negative amounts for debits and positive for credits
        if transaction_type in ['debit', 'withdrawal']:
            transaction_amount = abs(transaction_amount) * -1
        else:
            transaction_amount = abs(transaction_amount)

        record = {
            'transaction_id': transaction_id,
            'customer_id': customer_id,
            'transaction_amount': transaction_amount,
            'transaction_type': transaction_type,
            'account_type': account_type,
            'transaction_status' : transaction_status,
            'currency': currency,
            'merchant': merchant,
            'year': year,
            'month': month,
            'day': day,
            'hour': hour,
            'minutes': minutes
        }

        financial_data.append(record)

    return financial_data
