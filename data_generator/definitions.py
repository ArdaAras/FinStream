# This file holds data definitions

import random as random
from faker import Faker

fake_generator = Faker()

def load_merchants() -> list:
    """
    Reads the merchants from a file and returns a list of merchants.
    """
    with open('.\data_generator\merchants.txt', 'r') as file:
        lines = file.readlines()

    # Remove newline characters
    lines = [line.strip() for line in lines]

    return lines

customer_ids = [random.randint(1,1000) for _ in range(1000)]
transaction_types = ['debit', 'credit', 'transfer', 'withdrawal']
account_types = ['savings', 'checking', 'credit card', 'investment']
transaction_statuses = ["Success", "Failed", "Pending"]
currencies = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD']
merchants = load_merchants()
