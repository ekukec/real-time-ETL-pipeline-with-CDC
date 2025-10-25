from datetime import datetime
import random

class Customer:
    TIERS = ["Bronze", "Silver", "Gold", "Diamond"]

    @staticmethod
    def create(customer_id: str, name: str, tier: str = None) -> dict:
        return {
            "customer_id": customer_id,
            "name": name,
            "tier": tier or random.choice(Customer.TIERS),
            "created_at": datetime.utcnow()
        }

class Order:
    STATUSES = ["pending", "processing", "shipped", "delivered"]

    @staticmethod
    def create(order_id: str, customer_id: str, amount: float, status: str = "pending") -> dict:
        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": amount,
            "status": status,
            "created_at": datetime.utcnow()
        }