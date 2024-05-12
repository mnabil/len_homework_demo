from faker import Faker
import json
import random
from typing import List, Dict, Union

def generate_product_data(fake: Faker, num_products: int) -> List[Dict[str, Union[str, float]]]:
    """
    This function generates mock product data using the fake object from the Faker library and a specified number of products. Each product consists of a unique product ID, a product name, and a price.

    Parameters:
    fake: A Faker object used to generate fake data.
    num_products: An integer specifying the number of products to generate.

    Returns:
        products: A list of dictionaries, where each dictionary represents a product with the following keys:
            "product_id": A string representing a unique product ID.
            "product_name": A string representing the product name.
            "price": A float representing the price of the product.
    """
    products = []
    product_ids = set()
    for _ in range(num_products):
        # Generate unique product ID
        product_id = fake.unique.uuid4()
        while product_id in product_ids:
            product_id = fake.unique.uuid4()
        product_ids.add(product_id)
        
        # Generate product name and short name
        product_name = fake.word().capitalize() + " " + fake.word().capitalize()
        
        # Generate random price
        price = round(random.uniform(1, 1000), 2)
        
        product = {
            "product_id": str(product_id),
            "product_name": product_name,
            "price": price
        }
        products.append(product)
    return products

# Init Faker object
fake = Faker()

# Generate product data
products = generate_product_data(fake, 10000)

# Save products to a file
with open("products.json", "w") as f:
    json.dump(products, f, indent=4)

# ############################################################################ TABLE CREATION SQL ############################################################################
# create or replace TABLE STAGING.PUBLIC.PRODUCTS (
# 	PRICE NUMBER(38,2),
# 	PRODUCT_ID VARCHAR(16777216),
# 	PRODUCT_NAME VARCHAR(16777216)
# );