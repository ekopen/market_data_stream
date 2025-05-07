from kafka import KafkaConsumer
consumer = KafkaConsumer('price_ticks', bootstrap_servers='localhost:9092')
print("Connected!")