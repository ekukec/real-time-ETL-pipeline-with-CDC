from pymongo import MongoClient
from src.common.config import config
from src.data_generator.models import Customer, Order
import random

class DataSeeder:
    def __init__(self):
        self.client = MongoClient(config.MONGODB_URI)
        self.db = self.client[config.MONGODB_DATABASE]
        self.customers = self.db[config.CUSTOMERS_COLLECTION]
        self.orders = self.db[config.ORDERS_COLLECTION]

    def clear_collections(self):
        print("Clearing existing data...")
        self.customers.delete_many({})
        self.orders.delete_many({})
        print("Collections cleared")

    def seed_customers(self, count: int = 50):
        print(f"\nCreating {count} customers...")

        first_names = ["John", "Jane", "Mike", "Sarah", "David", "Emma", "Chris", "Lisa", "Tom", "Anna", "Emanuel"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]

        customers_list = []
        for i in range(1, count + 1):
            first = random.choice(first_names)
            last = random.choice(last_names)

            customer = Customer.create(
                customer_id=f"CUSTOMER-{i}",
                name=f"{first} {last}",
                tier=random.choice(Customer.TIERS)
            )
            customers_list.append(customer)

        result = self.customers.insert_many(customers_list)
        print(f"Created {len(result.inserted_ids)} customers")

        return customers_list

    def seed_orders(self, customers: list, orders_per_customer: int = 3):
        total_orders = len(customers) * orders_per_customer
        print(f"\nCreating {total_orders} orders...")

        orders_list = []
        order_counter = 1

        for customer in customers:
            num_orders = random.randint(1, orders_per_customer + 2)

            for _ in range(num_orders):
                amount = round(random.uniform(10.0, 500.0), 2)

                status_weights = {
                    "delivered": 0.6,
                    "shipped": 0.2,
                    "processing": 0.15,
                    "pending": 0.05
                }
                status = random.choices(
                    list(status_weights.keys()),
                    weights=list(status_weights.values())
                )[0]

                order = Order.create(
                    order_id=f"ORDER-{order_counter}",
                    customer_id=customer["customer_id"],
                    amount=amount,
                    status=status
                )

                orders_list.append(order)
                order_counter += 1

        result = self.orders.insert_many(orders_list)
        print(f"Created {len(result.inserted_ids)} orders")

        return orders_list

    def close(self):
        self.client.close()


def main():
    print("Starting data seeding process...\n")

    seeder = DataSeeder()

    try:
        seeder.clear_collections()

        customers = seeder.seed_customers(count=50)

        seeder.seed_orders(customers, orders_per_customer=3)

        print("\nSeeding complete!")

    except Exception as e:
        print(f"\nError during seeding: {e}")
        raise
    finally:
        seeder.close()


if __name__ == "__main__":
    main()