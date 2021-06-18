# LMDBIndexer

This is a DBMS-type Jina indexer, using [lmdb](https://lmdb.readthedocs.io/en/release/) as a backend. 

`lmdb` is a disk-based key-value storage system. It is quite performant. The test `test_lmdb_crud` in `tests/` ran with 100k docs in 1m 3secs

## Usage


```python
indexer = LMDBIndexer(
    map_size = 10485760,  
    default_traversal_path = 'r',
)
```

Parameters:

- `map_size` : maximum size of the database on disk
- `default_traversal_path`: the default traversal path for the `DocumentArray` in a request. Can be overridden with `parameters={'traversal_path': ..}` 

Check [tests](tests/test_lmdb.py) for more usage scenarios.

