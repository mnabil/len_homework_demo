from faker import Faker
from datetime import datetime
import json
import random
import string
import hashlib
from typing import List, Dict, Union

def generate_user_info(fake: Faker, num_users: int) -> List[Dict[str, Union[str, int, datetime]]]:
    """
    This function generates mock user information using the provided fake object from the Faker library and a specified number of users. Each user is represented by a dictionary containing various details such as username, password, name, address, email, phone number, birthdate, registration date, and a unique user ID.

    Parameters:
    fake: A Faker object used to generate fake data.
    num_users: An integer specifying the number of users to generate.

    Returns:
        user_info: A list of dictionaries, where each dictionary represents a user with the following keys:
            "id": A string representing a unique user ID.
            "first_name": A string representing the user's first name.
            "last_name": A string representing the user's last name.
            "address": A string representing the user's address.
            "email": A string representing the user's email address.
            "phone_number": A string representing the user's phone number.
            "birthdate": A string representing the user's birthdate.
            "registered_on": A string representing the user's registration date.
            "username": A string representing the user's username.
            "password": A string representing the user's password.
    """
    user_info = []
    for _ in range(num_users):
        # Generate random username
        username = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
        # Generate random password
        password = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(12))
        
        # Generate unique user ID
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        user_id = hashlib.md5((first_name + last_name + email).encode()).hexdigest() # Generate unique user ID
        user = {
            "id": user_id,    
            "first_name": first_name,
            "last_name": last_name,
            "address": fake.address(),
            "email": email,
            "phone_number": fake.phone_number(),
            "birthdate": str(fake.date_of_birth(minimum_age=18, maximum_age=90)),
            "registered_on": str(fake.date_time_between_dates(datetime(2023, 1, 1), datetime.today())),
            "username": username,
            "password": password
        }
        user_info.append(user)
    return user_info

# Init Faker object
fake = Faker()

# Generate 100 users and save to a file
users = generate_user_info(fake, 1000)
with open("user_info.json", "w") as f:
    json.dump(users, f)


# ############################################################################ TABLE CREATION SQL ############################################################################
# create or replace TABLE STAGING.PUBLIC.USERS (
# 	ADDRESS VARCHAR(16777216),
# 	BIRTHDATE DATE,
# 	EMAIL VARCHAR(16777216),
# 	FIRST_NAME VARCHAR(16777216),
# 	ID VARCHAR(16777216),
# 	LAST_NAME VARCHAR(16777216),
# 	PASSWORD VARCHAR(16777216),
# 	PHONE_NUMBER VARCHAR(16777216),
# 	REGISTERED_ON TIMESTAMP_NTZ(9),
# 	USERNAME VARCHAR(16777216)
# );
