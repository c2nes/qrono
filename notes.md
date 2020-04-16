
* Primary queue index is simple merge sorted collection
* Stable PK allocated on initial insert

Is a primary key index necessary?

278 bytes per message
1,000,000 messages

Raw size = 265MB
Log size = 329MB
Overhead = 64MB
Per Item = ~67 bytes

Ideas,
* Add an epoch to each segment and store deadlines as offsets
* Delta encode IDs and/or deadlines
* Store IDs as deltas from smallest ID
