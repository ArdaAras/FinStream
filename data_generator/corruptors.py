# This file contains data corruptor functions to corrupt the data in different ways.

import random

def announcer(func):
    def wrapper(*args, **kwargs):
        # Announces the name of the function being called
        print(f"Corrupting the record with '{func.__name__}'")
        return func(*args, **kwargs)
    return wrapper

@announcer
def set_none(record: dict):
    # Selects a key from the given dictionary randomly and sets it to None
    key_to_corrupt = random.choice(list(record.keys()))
    record[key_to_corrupt] = None

@announcer
def set_negative_customer_id(record: dict):
    # Sets customer_id to a negative value
    record['customer_id'] = -record['customer_id']

@announcer
def set_invalid_transaction_type(record: dict):
    # Sets transaction_type to an invalid value
    record['transaction_type'] = 'invalid_type'

@announcer
def set_unknown_currency(record: dict):
    # Sets currency to an invalid value
    record['currency'] = 'unknown_currency'

@announcer
def corrupt_date(record: dict):
    # Replaces correct 'year', 'month' or 'day' keys with out-of-range values
    key_to_corrupt = random.choice(['year', 'month', 'day'])
    record[key_to_corrupt] = random.randint(13, 20) if key_to_corrupt == 'month' else random.randint(31, 41) if key_to_corrupt == 'day' else random.randint(1600, 1800)

def corrupt_data(records: list):
    # With a %5 chance, applies a random corruptor function to the given list of records
    corruptor_functions = [set_none, set_negative_customer_id, set_invalid_transaction_type, set_unknown_currency, corrupt_date]
    for record in records:
        if random.random() < 0.05:
            #print(f'Before:{record}')
            random.choice(corruptor_functions)(record)
            #print(f'After:{record}')
