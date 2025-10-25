import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    MONGODB_URI = os.getenv(
        "MONGODB_URI",
        "mongodb://localhost:27018/"
    )
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "etl_source")

    ORDERS_COLLECTION = "orders"
    CUSTOMERS_COLLECTION = "customers"

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_ORDERS = "orders-cdc"
    KAFKA_TOPIC_CUSTOMERS = "customers-cdc"

    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "warehouse")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

    @classmethod
    def validate(cls):
        required = ["MONGODB_URI", "MONGODB_DATABASE"]
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            raise ValueError(f"Missing required config: {missing}")


config = Config()
config.validate()