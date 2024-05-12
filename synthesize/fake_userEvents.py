from faker import Faker
import json
import random
from datetime import datetime
from typing import List, Dict, Union

# Load users and products data
with open("user_info.json", "r") as f:
    users = json.load(f)

with open("products.json", "r") as f:
    products = json.load(f)

def generate_user_activity(fake: Faker, 
                           users: List[Dict[str, Union[int, str, datetime]]], 
                           products: List[Dict[str, Union[str, float]]], 
                           num_events: int) -> List[Dict[str, Union[str, int]]]:
    """
    This function generates mock user activity data using the provided users, products, and a specified number of events. Each event includes details such as the event type, event time, and additional activity details based on the event type.

    Parameters:
    fake: A Faker object used to generate fake data.
    users: A list of dictionaries representing user data. Each dictionary should have keys such as "id", "username", "password", and "registered_on".
    products: A list of dictionaries representing product data. Each dictionary should have keys such as "product_id", "product_name", and "price".
    num_events: An integer specifying the number of events to generate.

    Returns:
        activities: A list of dictionaries, where each dictionary represents a user activity event with the following keys:
            "event_type": A string representing the type of event (e.g., "login", "view", "add_to_cart", "purchase").
            "event_time": A string representing the time of the event.
            "activity_details": A string containing additional details specific to the event type.
    """
    activities = []
    referrals = [fake.url() for _ in range(40)] + [None] # referral links
    for _ in range(num_events):
        user = random.choice(users)
        product = random.choice(products)
        # to get event time between user registration and date of today to avoid logical errors
        event_time = fake.date_time_between_dates(datetime.fromisoformat(user['registered_on'].replace("Z", "+00:00")), datetime.today()).isoformat()
        event_type = random.choice(["login", "view", "add_to_cart", "purchase"])

        activity = {
            "event_type": event_type,
            "event_time": event_time,
            "activity_details": "" # additional details based on event type
        }
        match event_type:
            case "login":
                username = user["username"]
                password = user["password"]
                ip_address = fake.ipv4()
                activity["activity_details"] = "    "+ json.dumps({
                    "user_id": user["id"],
                    "username": username,
                    "password": password,
                    "ip_address": ip_address,
                    "user_agent": fake.user_agent(),
                })
            case "view":
                logged_in = random.choice([True, False])
                activity["activity_details"] = "    "+ json.dumps({
                    "referral": random.choice(referrals),
                    "user_agent": fake.user_agent(),
                    "price": product["price"],
                    "logged_in": logged_in,
                    "user_id": user["id"] if logged_in else None,
                    "product_id": product["product_id"],

                })
            case "add_to_cart":
                quantity = random.randint(1, 10)
                activity["activity_details"] = "    "+ json.dumps({
                    "quantity": quantity,
                    "product_id": product["product_id"],
                    "user_id": user["id"]
                })
            case "purchase":
                quantity = random.randint(1, 10)
                activity["activity_details"] = "    "+ json.dumps({
                    "user_id": user["id"],
                    "product_id": product["product_id"], # could be multiple products
                    "status": random.choice(["success", "failed"]),
                    "total_amount": product["price"]*quantity,
                    "quantity": quantity,
                })
        activities.append(activity)
    return activities

# Init Faker object
fake = Faker()

# Generate user activity events
user_activity = generate_user_activity(fake, users, products, 50000)

# Save user activity to a file
with open("user_activity.json", "w") as f:
    json.dump(user_activity, f, indent=4)

## ############################################################################ TABLE CREATION SQL ############################################################################
# create or replace TABLE STAGING.PUBLIC.USER_ACTIVITY (
# 	ACTIVITY_DETAILS VARIANT,
# 	EVENT_TIME TIMESTAMP_NTZ(9),
# 	EVENT_TYPE VARCHAR(16777216)
# );