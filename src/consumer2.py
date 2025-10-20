#! /usr/bin/env python3

from kafka import KafkaConsumer
import json
import csv
import time

# Configuration
BROKER_IP = '172.24.224.84:9092'
NET_TOPIC = 'topic-net'
DISK_TOPIC = 'topic-disk'
NET_OUTPUT = 'net_data.csv'
DISK_OUTPUT = 'disk_data.csv'

def consume_messages():
    # Create consumers for both topics
    consumer = KafkaConsumer(
        NET_TOPIC,
        DISK_TOPIC,
        bootstrap_servers=[BROKER_IP],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-2'
    )
    
    # Open CSV files
    net_file = open(NET_OUTPUT, 'w', newline='')
    disk_file = open(DISK_OUTPUT, 'w', newline='')
    
    net_writer = csv.writer(net_file)
    disk_writer = csv.writer(disk_file)
    
    # Write headers
    net_writer.writerow(['ts', 'server_id', 'net_in', 'net_out'])
    disk_writer.writerow(['ts', 'server_id', 'disk_io'])
    
    print("Consumer 2 started. Consuming Network and Disk data...")
    
    try:
        message_count = 0
        for message in consumer:
            data = message.value
            
            if message.topic == NET_TOPIC:
                net_writer.writerow([data['ts'], data['server_id'], 
                                    data['net_in'], data['net_out']])
            elif message.topic == DISK_TOPIC:
                disk_writer.writerow([data['ts'], data['server_id'], data['disk_io']])
            
            message_count += 1
            if message_count % 100 == 0:
                print(f"Consumed {message_count} messages...")
                net_file.flush()
                disk_file.flush()
                
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        net_file.close()
        disk_file.close()
        consumer.close()
        print(f"Total messages consumed: {message_count}")

if __name__ == "__main__":
    consume_messages()