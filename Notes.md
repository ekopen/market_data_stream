
# Standout Engineering Features
1. Modularity. We can use whatever technology we want for the storage tables and this code can adapt
2. Speed/Batching. needed for kakfa producer
3. Threading. the data stream is heavy and any additionally logging can slow it down, so we need to use threading
4. Queues for diagnostics, so make them threadsafe
5. time zone issues between differnt tableas

# Next steps

- fix diagnostics with simplified approach

- finish logging error database/ all logs
    - may need to add extra try loops and logs across the board
    - restart for errors if needed
- log downtime too
- figure out how to run on cloud
- create flow chart

- next next steps:
    - reconfigure for bid/ask with binance data
    - add a trading strategies setction
        - maybe do automatic arbitrage strategies, and then discretionary strategies