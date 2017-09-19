import argparse
import socket
import struct
import sys
import json
import time as time_

import argparse
import socket
import struct
import sys
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

MCAST_GRP = '224.1.1.1'
MCAST_PORT = 8087

class Connection:
    def __init__(self):
        self.topic = 'indigo-json'
        pass

    # call this with a LIST of host:port strings
    def connect(self, hostports):
        indigo.server.log(u'starting kafka connection')
        servers = hostports.split(',')
        if (len(servers) < 2):
            servers = [hostports]
        self.producer = KafkaProducer(
            bootstrap_servers = servers,
            value_serializer = lambda m: json.dumps(m).encode('utf-8')
        )
        indigo.server.log(u'kafka connection succeeded')

    def syncsend(self, what):
        try:
            future = self.producer.send(self.topic, what)
            record_metadata = future.get(timeout=10)
        except KafkaError:
            pass

    def send(self, what):
        try:
            self.producer.send(self.topic, what)
        except KafkaError:
            pass

    def stop(self):
        indigo.server.log(u'stopping kafka connection')
        self.producer.flush(timeout=5)

class Receiver:
    def __init__(self):
        # todo config block
        self.topic = 'indigo-json'
        self.brokers = 'localhost:9092'
        self.mcastport = str(MCAST_PORT)

        self.connection = None

    def connect(self):
        print(u'Starting socket')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.sock.bind(('', int(self.mcastport)))
        mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        print(u'Starting kafka connection')

        try:
            self.connection = Connection()
            self.connection.connect(self.brokers)
            self.connection.topic = self.topic
        except:
            print(u'Unable to connect!')
            return -1

        print(u'Kafka connection succeeded')

    def send(self, strwhat):
        json_body = json.loads(strwhat)[0]['fields']
        json_body[u'timestamp'] = int(time_.time())
        print(json.dumps(json_body).encode('utf-8'))
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
            print('Server stopped.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--multicastport', help='muticast port', default='8087')
    parser.add_argument('-b', '--brokers', help='broker connection string', default='localhost:9092')
    parser.add_argument('-t', '--topic', help='topic', default='indigo-json')
    args, unknown = parser.parse_known_args()

    ir = Receiver()
    ir.brokers = args.brokers
    ir.topic = args.topic
    ir.mcastport = args.multicastport

    if ir.connect() != 0:
        sys.exit(-1)
    ir.run()

