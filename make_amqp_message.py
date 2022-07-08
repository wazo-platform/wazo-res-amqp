#!/usr/bin/env python

# Creates and sends a message to an AMQP client

from kombu import Connection, Exchange, Producer
import argparse

parser = argparse.ArgumentParser(description='Generate messages')
parser.add_argument('-r', '--route', default='key')
parser.add_argument('-m', '--message', default='hello, there!')
args = parser.parse_args()
rabbit_url = "amqp://localhost:5672/"

conn = Connection(rabbit_url)

channel = conn.channel()
exchange = Exchange("xivo", type="topic")
producer = Producer(exchange=exchange, channel=channel, routing_key=args.route)
producer.publish(args.message)