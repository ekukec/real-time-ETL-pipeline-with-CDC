import time
from datetime import datetime
import random

from pymongo import MongoClient

from src.common.config import config
from src.data_generator.models import Order, Customer


class DataGenerator:
    def __init__(self):
        self.client = MongoClient(config.MONGODB_URI)
        self.db = self.client[config.MONGODB_DATABASE]
        self.orders = self.db[config.ORDERS_COLLECTION]
        self.customers = self.db[config.CUSTOMERS_COLLECTION]

        self.order_counter = self.get_next_order_number()

    def get_next_order_number(self):
        last_order = self.orders.find_one(
            sort=[('order_id', -1)],
        )

        if last_order and 'order_id' in last_order:
            try:
                last_order_number = int(last_order['order_id'].split('-')[1])
                return last_order_number + 1
            except:
                pass

        return 100000

    def create_random_order(self):
        all_customers = list(self.customers.find())

        if not all_customers:
            print("No customers found, run seeder first")
            return

        customer = random.choice(all_customers)

        amount = round(random.uniform(10.0, 500.0), 2)

        order = Order.create(
            order_id=f"ORDER-{self.order_counter}",
            customer_id=customer['customer_id'],
            amount=amount,
            status="pending"
        )

        result = self.orders.insert_one(order)

        self.order_counter += 1
        return result.inserted_id

    def update_order_status(self):
        status_flow = {
            "pending": "processing",
            "processing": "shipped",
            "shipped": "delivered"
        }

        possible_statuses = ["pending", "processing", "shipped"]

        order = self.orders.find_one({
            "status": {"$in": possible_statuses}
        })

        if not order:
            print("No orders to update")
            return

        current_status = order['status']
        new_status = status_flow.get(current_status)

        if new_status:
            self.orders.update_one(
                {"_id": order['_id']},
                {"$set": {
                    "status": new_status,
                    "updated_at": datetime.utcnow()
                }}
            )

            print(f"Updated status for order {order['_id']}")

    def update_customer_tier(self):
        customer = self.customers.find_one()

        if not customer:
            print("No customers found, run seeder first")
            return

        new_tier = random.choice(Customer.TIERS)

        if new_tier != customer['tier']:
            self.customers.update_one(
                {"_id": customer['_id']},
                {"$set": {
                    "tier": new_tier,
                    "updated_at": datetime.utcnow()
                }}
            )

            print(f"Updated tier for customer {customer['_id']} to {new_tier}")

    def run(self, new_order_interval: int = 5, update_order_interval: int = 8, update_customer_tier_interval: int = 27):
        last_new_order = time.time()
        last_order_update = time.time()
        last_customer_tier_update = time.time()

        try:
            while True:
                current_time = time.time()

                if current_time - last_new_order > new_order_interval:
                    self.create_random_order()
                    last_new_order = current_time

                if current_time - last_order_update > update_order_interval:
                    self.update_order_status()
                    last_order_update = current_time

                if current_time - last_customer_tier_update > update_customer_tier_interval:
                    self.update_customer_tier()
                    last_customer_tier_update = current_time

                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping execution")
        finally:
            self.close()

    def close(self):
        self.client.close()

def main():
    generator = DataGenerator()

    generator.run()

if __name__ == '__main__':
    main()