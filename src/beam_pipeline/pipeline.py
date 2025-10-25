import json
from datetime import datetime

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2

from src.common.config import config


class ParseKafkaMessage(beam.DoFn):
    def process(self, element):
        try:
            _, value = element
            message = json.loads(value.decode('utf-8'))

            if message['operation'] in ['insert', 'update']:
                yield message
        except Exception as e:
            print(f"Error parsing kafka message: {e}")

class EnrichOrderWithCustomer(beam.DoFn):
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.mongo_client = None

    def setup(self):
        from pymongo import MongoClient
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client[self.db_name]
        print('Mongo client setup complete')

    def process(self, element):
        try:
            order_data = element.get('data', {})

            if not order_data:
                print('no order data')
                return

            customer_id = order_data.get('customer_id')
            customer = self.db.customers.find_one({'customer_id': customer_id})

            print(f"order id: {order_data.get('order_id')}")

            enriched_order = {
                'order_id': order_data.get('order_id'),
                'customer_id': customer_id,
                'customer_name': customer.get('name', 'Unknown') if customer else 'Unknown',
                'customer_tier': customer.get('tier', 'Bronze') if customer else 'Bronze',
                'amount': order_data.get('amount', 0.0),
                'status': order_data.get('status', 'unknown'),
                'order_created_at': order_data.get('created_at'),
                'order_updated_at': order_data.get('updated_at'),
                'cdc_operation': element.get('operation'),
                'processed_at': datetime.utcnow().isoformat()
            }

            yield enriched_order
        except Exception as e:
            print(f"Error enriching order with customer: {e}")

    def teardown(self):
        if self.mongo_client:
            self.mongo_client.close()

class WriteToPostgres(beam.DoFn):
    def __init__(self, postgres_config):
        self.postgres_config = postgres_config
        self.conn = None

    def setup(self):
        self.conn = psycopg2.connect(**self.postgres_config)
        print('Postgres connection established')

    def process(self, element):
        try:
            cursor = self.conn.cursor()

            query ="""
                INSERT INTO orders (order_id, customer_id, customer_name, customer_tier,
                    amount, status, order_created_at, order_updated_at,
                    cdc_operation, processed_at
                ) VALUES (
                    %(order_id)s, %(customer_id)s, %(customer_name)s, %(customer_tier)s,
                    %(amount)s, %(status)s, %(order_created_at)s, %(order_updated_at)s,
                    %(cdc_operation)s, %(processed_at)s
                )
                ON CONFLICT (order_id) 
                DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    customer_tier = EXCLUDED.customer_tier,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    order_updated_at = EXCLUDED.order_updated_at,
                    cdc_operation = EXCLUDED.cdc_operation,
                    processed_at = EXCLUDED.processed_at
            """

            cursor.execute(query, element)
            self.conn.commit()

            cursor.close()

            yield element
        except Exception as e:
            print(f"Error writing to postgres: {e}")

    def teardown(self):
        if self.conn:
            self.conn.close()

def run_pipeline():
    print('Starting pipeline')

    options = PipelineOptions(
        ['--runner=DirectRunner', '--streaming'],
    )

    postgres_config = {
        'host': config.POSTGRES_HOST,
        'port': config.POSTGRES_PORT,
        'database': config.POSTGRES_DB,
        'user': config.POSTGRES_USER,
        'password': config.POSTGRES_PASSWORD,
    }

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Read from kafka' >> ReadFromKafka(
                consumer_config={
                    'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
                    'group.id': 'beam-consumer-group',
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': 'true'
                },
                topics=[config.KAFKA_TOPIC_ORDERS],
                max_num_records=20,
            )

            | 'Parse messages' >> beam.ParDo(ParseKafkaMessage())

            | 'Enrich order' >> beam.ParDo(EnrichOrderWithCustomer(config.MONGODB_URI, config.MONGODB_DATABASE))

            | 'Write to postgres' >> beam.ParDo(WriteToPostgres(postgres_config))
        )

    print('Finished pipeline')

if __name__ == '__main__':
    run_pipeline()