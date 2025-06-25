import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class PurchasingStreamProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("PurchasingStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def create_kafka_stream(self, kafka_servers='localhost:9092', topic='purchasing-events'):
        """Create Kafka streaming DataFrame"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def process_purchasing_events(self, df):
        """Process purchasing events and calculate daily aggregations"""
        
        # Define schema for purchasing events
        purchase_schema = StructType([
            StructField("event_id", StringType()),
            StructField("user_id", StringType()),
            StructField("product_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("timestamp", StringType()),
            StructField("currency", StringType())
        ])
        
        # Parse JSON from Kafka
        parsed_df = df.select(
            from_json(col("value").cast("string"), purchase_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp 
        processed_df = parsed_df \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
            .withWatermark("event_timestamp", "10 minutes")
        
        # Aggregate daily totals
        daily_aggregated = processed_df \
            .groupBy("date") \
            .agg(
                sum("amount").alias("daily_total"),
                count("*").alias("transaction_count")
            )
        
        return daily_aggregated
    
    def output_to_console_with_foreach_batch(self, df):
        """Output using foreachBatch with running total calculation"""
        
        # Global variable to maintain running total
        running_total = [0.0]
        
        def process_batch(batch_df, batch_id):
            if batch_df.count() > 0:
                # Calculate current batch total
                current_batch_total = batch_df.agg(sum("daily_total")).collect()[0][0] or 0.0
                running_total[0] += current_batch_total
                
                # Create output with timestamp and running total
                output_data = [
                    {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "running_total": running_total[0],
                        "batch_id": batch_id,
                        "current_batch_total": current_batch_total
                    }
                ]
                
                output_df = self.spark.createDataFrame(output_data)
                print(f"=== Batch {batch_id} ===")
                output_df.show(truncate=False)
                
                # show detailed batch data
                print("Batch Details:")
                batch_df.show(truncate=False)
        
        return df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("update") \
            .trigger(processingTime='10 seconds') \
            .start()
    
    def output_to_console_complete_mode(self, df):
        """Output using complete mode"""
        return df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='10 seconds') \
            .start()
    
    def run_streaming_job(self):
        """Run the complete streaming job"""
        print("Starting Purchasing Stream Processor...")
        
        # Create Kafka stream
        kafka_df = self.create_kafka_stream()
        
        # Process purchasing events
        processed_df = self.process_purchasing_events(kafka_df)
        
        # Start streaming with foreachBatch
        query1 = self.output_to_console_with_foreach_batch(processed_df)
        
        # Alternative: Complete mode output (uncomment to use)
        # query2 = self.output_to_console_complete_mode(processed_df)
        
        try:
            query1.awaitTermination()
            # query2.awaitTermination()  # if using complete mode
        except KeyboardInterrupt:
            print("Stopping streaming job...")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    processor = PurchasingStreamProcessor()
    processor.run_streaming_job()