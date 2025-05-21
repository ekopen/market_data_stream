# Potential Issues
Here are some issues I have diagnosed but not figured out how to fix:
1. The transfer between hot and warm has an irregular rate per batch. This could be due to some bottle neck in the code. It cold also be due to Binance or Finhub having some sort of batching system.
2. no data to fetch error in warm to cold?

# Improvement Ideas

# Standout Engineering Features
1. Modularity. We can use whatever technology we want for the storage tables and this code can adapt
2. Speed/Batching. needed for kakfa producer
3. Threading. the data stream is heavy and any additionally logging can slow it down, so we need to use threading

# Next steps
- finish logging error database
    - may need to add extra try loops and logs across the board
- show the new loggs on streamlit from diagnostics
- figure out how to run on cloud