# Jina Indexers

This repository contains a selection of Executors for Jina 2.0.
They are to be used for storing or retrieving your data.
They are referred to as Indexers.

They are split by usage and interface. The types are:

1. [indexers](./jinahub/indexers/indexer). This category is for *storing* data, in a CRUD-like interface. These Executors are reliable and performant in write/update/delete operations. They are *NOT* meant to be used in search. For that we have:
1. [searchers](./jinahub/indexers/searcher) This category of Executors are to be used at search time.
    1. [vector-based](./jinahub/indexers/searcher/vector) These usually implement a form of similarity search, based on the embeddings created by the encoders you have chosen in your Flow.
    1. [metadata](./jinahub/indexers/searcher/keyvalue) These are simple interfaces for key-value lookup. They are meant to be used to retrieve the full metadata of a `Document`, based on the results from the vector-based searchers above.
    1. [compound](./jinahub/indexers/searcher/compound) These are compound classes, usually made up of a vector-based and a key-value searcher.

## Indexing vs Searching Operations

The recommended usage of these Executors is to split them into Indexing vs Search Flows.
In the Indexing Flow, you perform write, update, and delete. 
In order to search them, you need to start a Search Flow, dump the data from the Index Flow, and load it into the Query Flow.

See below figure for how this would look like:

![](./.github/img/replicas.jpg)

In the above case, the DBMS could be the [PostgreSQL](./jinahub/indexers/indexer/PostgreSQLIndexer)-based Indexer, while the Query Flow could be based on [NumpyPostgresSearcher](./jinahub/indexers/searcher/compound/NumpyPostgresSearcher).

For a showcase code, check our [integration tests](./jinahub/indexers/tests/integration/psql_dump_reload).

The split between indexing and search Flows allows you to continuously serve requests in your application (in the search Flow), while still being able to write or modify the underlying data. Then when you want to update the state of the searchable data for your users, you perform a dump and rolling update.

### Dump and Rolling Update

The communication between index and search Flows is done via this pair of actions.
The **dump** action tells the indexers to export its internal data (from whatever format it stores it in) to a disk location, optimized to be read by the shards in your search Flow.
At the other end, the **rolling update** tells the search Flow to recreate its internal state with the new version of the data.

Looking at the [test](./jinahub/indexers/tests/integration/psql_dump_reload/test_dump_psql.py), we can see how this is called:

```python
flow_dbms.post(
     on='/dump',
     target_peapod='indexer_dbms',
     parameters={
         'dump_path': dump_path,
         'shards': shards,
         'timeout': -1,
     },
 )
```

where

- `flow_dbms` is the Flow with the storage Indexer
- `target_peapod` is the name of the executor, defined in your `flow.yml`
- `dump_path` is the path (on local disk) where you want the data to be stored
- `shards` is the nr of shards you have in your search Flow. **NOTE** This doesn't change the value in the Flow. You need to keep track of how you configured your search Flow

For performing the **rolling update**, we can see the usage in the same test:

```python
flow_query.rolling_update(pod_name='indexer_query', dump_path=dump_path)
```

where

- `flow_query` is the Flow with the searcher Indexer
- `pod_name` is the name of the executor, defined in your `flow.yml`
- `dump_path` is the folder where you exported the data, from the above **dump** call

### Notes

- `dump_path` needs to be accessible by local reference. It can however be a network location / internal Docker location that you have mapped 
