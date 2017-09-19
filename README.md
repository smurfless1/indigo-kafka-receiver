# indigo-kafka-receiver

Receive device changes via json from the Indigo json plugin, and broadcast them to a Kafka topic.

Args:

* -t or --topic : default indigo-json
* -b or --brokers : broker list in format host:port, default localhost:9092
* -m or --multicastport : the multicast port to connect to, default 8087, which is the same as the indigo-json-broadcast default

