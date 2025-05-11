# Potential Issues
Here are some issues I have diagnosed but not figured out how to fix:
1. The transfer between hot and warm has an irregular rate per batch. This could be due to some bottle neck in the code. It cold also be due to Binance or Finhub having some sort of batching system.

# Improvement Ideas
1. Make sure that the websocket flow is real time. Log the rate of data coming in and flow between tables, catching errors where there are any.