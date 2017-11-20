# Indexing part of the Wrble search engine, based on bleve.

Currently it's specific to Wrble's implementation and Cassandra is the only supported backend but hope to some day make it general purpose again.

## TODO:

- Term Frequency Row, add score to key so we can order by them and stop searches early
- Doc length
- Average doc length
- Document boosting
- Atomic boost/field updates
- Better Cassandra sharding
- Custom tables (t - custom columns, b - sharding)
- Makefile

## DONE:

- BM25
- Custom 'd' table using counters
- Re-introduce range iterators
- Fix broken phrase search
- Basic indexing and search functional
- Make the KV store table aware
- Remove some operations (like match-all) that won't work in a large distributed index
