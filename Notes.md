# Potential Issues
Here are some issues I have diagnosed but not figured out how to fix:
1. The transfer between hot and warm has an irregular rate per batch. This could be due to some bottle neck in the code. It cold also be due to Binance or Finhub having some sort of batching system.
2. Shut downs are not working. Also docker needs to be reset everytime i shut down.

# Improvement Ideas
1. Make sure that the websocket flow is real time. Log the rate of data coming in and flow between tables, catching errors where there are any. I do NOT think we are getting real time data
2. The batching in data ingestion is really important, make that better? create a log of lag between Kafka and Clickhouse

# Standout Engineering Features
1. Modularity. We can use whatever technology we want for the storage tables and this code can adapt
2. Speed/Batching.