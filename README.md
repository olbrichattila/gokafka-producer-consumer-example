# Golang Kafka producer and consumer

1. Spin up the docker container with docker-compose up
2. Run the producer from the producer folder, this will create a topic with 3 partition and push events round robin.
3. Run the consumer 1 and more instances and observe how Kafka distributes the events form wich partitions.

