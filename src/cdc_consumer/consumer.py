import json
from datetime import datetime

from kafka import KafkaProducer
from pymongo import MongoClient

from src.common.config import config


class Consumer:
    def __init__(self):
        self.mongo_client = MongoClient(config.MONGODB_URI)
        self.db = self.mongo_client[config.MONGODB_DATABASE]

        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        )

    def serialize_change_events(self, change_event: dict) -> dict:
        operation = change_event.get('operationType')
        document_key = change_event.get('documentKey', {})

        event = {
            'operation': operation,
            'timestamp': datetime.utcnow().isoformat(),
            'document_id': str(document_key.get('_id', '')),
            'collection': change_event.get('ns', {}).get('coll', ''),
        }

        if operation in ['insert', 'update', 'replace']:
            event['data'] = change_event.get('fullDocument', {})

            if '_id' in event['data']:
                event['data']['_id'] = str(event['data']['_id'])
        elif operation == 'delete':
            event['data'] = None

        return event

    def watch_collection(self, collection_name: str, kafka_topic: str):
        collection = self.db[collection_name]

        print(f"Watching collection: {collection_name}")
        print(f"Publishing to topic: {kafka_topic}")
        print("Waiting for changes...\n")

        try:
            with collection.watch(full_document='updateLookup') as change_stream:
                for event in change_stream:
                    event = self.serialize_change_events(event)

                    self.producer.send(kafka_topic, event)

                    operation = event.get('operation')
                    doc_id = event.get('document_id', 'unknown')

                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {operation.upper()}: {doc_id}")

                    print()

        except Exception as e:
            print(f"Error watching collection: {e}")
            raise

    def watch_orders(self):
        self.watch_collection(config.ORDERS_COLLECTION, kafka_topic=config.KAFKA_TOPIC_ORDERS)

    def watch_customers(self):
        self.watch_collection(config.CUSTOMERS_COLLECTION, kafka_topic=config.KAFKA_TOPIC_CUSTOMERS)

    def close(self):
        self.producer.close()
        self.mongo_client.close()

def main():
    consumer = Consumer()

    try:
        consumer.watch_orders()

    except KeyboardInterrupt:
        print("Shutting down consumer")
    except Exception as ex:
        print(f"Exception while consuming: {ex}")
        raise
    finally:
        consumer.close()

if __name__ == "__main__":
    main()