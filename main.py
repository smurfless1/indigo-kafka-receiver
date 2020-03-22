import json
import logging
import socket
import struct
import sys
import time as time_
from typing import Dict

import click

# Implementation libs
from kafka import KafkaProducer
from kafka.errors import KafkaError

MCAST_GRP = "224.1.1.1"
MCAST_PORT = 8087

logger = logging.getLogger("indigo-kafka-receiver")


class Connection:
    def __init__(self):
        self.topic = "indigo-json"
        self.producer = None

    def connect(self, hostports: str):
        """
        Call this with a single string, as a comma separated list of host:port strings.

        This is normal in the kafka world.

        :param hostports:
        :return:
        """
        logger.info("starting kafka connection")
        servers = hostports.split(",")
        if len(servers) < 2:
            servers = [hostports]
        self.producer = KafkaProducer(bootstrap_servers=servers, value_serializer=lambda m: json.dumps(m).encode("utf-8"))
        logger.info("kafka connection succeeded")

    def syncsend(self, what):
        try:
            future = self.producer.send(self.topic, what)
            future.get(timeout=10)
        except KafkaError:
            pass

    def send(self, what):
        try:
            self.producer.send(self.topic, what)
        except KafkaError:
            pass

    def stop(self):
        logger.info("stopping kafka connection")
        self.producer.flush(timeout=5)


class Receiver:
    def __init__(self):
        # todo config block
        self.topic: str = "indigo-json"
        self.brokers: str = "localhost:9092"
        self.mcastport: int = MCAST_PORT
        self.connection: Connection = None

    def connect(self) -> int:
        logger.info("Starting socket")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind(("", self.mcastport))
        mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        logger.info("Starting kafka connection")

        try:
            self.connection = Connection()
            self.connection.connect(self.brokers)
            self.connection.topic = self.topic
        except:  # noqa E722
            logger.info("Unable to connect!")
            return -1

        logger.info("Kafka connection succeeded")
        return 0

    def send(self, strwhat) -> None:
        json_body: Dict = json.loads(strwhat)[0]["fields"]
        json_body["timestamp"] = int(time_.time())
        logger.info(json.dumps(json_body).encode("utf-8"))
        try:
            self.connection.send(json_body)
        except KafkaError:
            pass

    def run(self):
        try:
            # Receive messages
            while True:
                data, addr = self.sock.recvfrom(10240)
                self.send(data)
        finally:
            # Close socket
            self.sock.close()
            logger.info("Server stopped.")


@click.command()
@click.option("--multicastport", "-m", type=int, default=MCAST_PORT, show_default=True, env="MCPORT", help="The multicast port.")
@click.option("--broker", type=str, default="localhost", show_default=True, help="The broker host to connect to.")
@click.option("--topic", type=str, default="indigo-json", show_default=True, help="The topic to write to.")
def main(multicastport, broker, topic):
    ir = Receiver()
    ir.brokers = broker
    ir.topic = topic
    ir.mcastport = multicastport

    if ir.connect() != 0:
        sys.exit(-1)
    ir.run()
