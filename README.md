# StreamingPipeline
SETUP INSTRUCTIONS:

1. Install Dependencies:
   pip install kafka-python pyspark

2. Start Kafka:
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka Server
   bin/kafka-server-start.sh config/server.properties
   
   # Create Topic
   bin/kafka-topics.sh --create --topic purchasing-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

3. Run Components:
   # Terminal 1: Start Producer
   python producer.py
   
   # Terminal 2: Start Streaming Job
   python streaming_job.py
   # OR
   python enhanced_streaming_job.py

4. Monitor Output:
   The streaming job will display:
   - Timestamp of each batch processing
   - Running total of all purchases
   - Batch details and transaction counts

EXPECTED OUTPUT FORMAT:
=============================================================
PURCHASING EVENTS SUMMARY - Batch 123
=============================================================
+-------------------+-------------+--------+----------------+------------+
|timestamp          |running_total|batch_id|new_transactions|batch_amount|
+-------------------+-------------+--------+----------------+------------+
|2024-01-15 10:30:45|1234.56      |123     |5               |234.56      |
+-------------------+-------------+--------+----------------+------------+
"""
