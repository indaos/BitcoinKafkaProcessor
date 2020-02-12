# BitcoinKafkaProcessor

<p>
The example of using Kafka to process a bitcoin database. 
</p>
<p>
This is a slightly unusual use of Kafka because there are no consumers, and Kafka is used only as a long-term storage. 
Bitcoin transactions are stored in KTables of RocksDB, which allows to make various analytical queries. 
</p>
<p>
Transactions are aggregated to account for the balance of each bitcoin address by day and the total volume of 
transactions for each day.
</p>
