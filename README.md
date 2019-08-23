# Learning Kafka

## Getting Started
`cd sample-app`
`virtualenv --python=3.6 make .venv`
`source .venv/bin/activate`
`pip install -r requirements.txt`

To start kafka/zookeeper: `cd broker && docker-compose up`
To run producer: `cd sample-app && ./run.sh producer.sh`
To run consumer: `cd sample-app && ./run.sh consumer.sh`
