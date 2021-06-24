# ✨ LMDBIndexer

**LMDBIndexer** is a Jina indexer, using [lmdb](https://lmdb.readthedocs.io/en/release/) as a backend. 

`lmdb` is a disk-based key-value storage system. It is quite performant. The test `test_lmdb_crud` in `tests/` ran with 100k docs in 1m 3secs

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [🌱 Prerequisites](#-prerequisites)
- [🚀 Usages](#-usages)
- [🎉️ Example](#%EF%B8%8F-example)
- [🔍️ Reference](#%EF%B8%8F-reference)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## 🌱 Prerequisites

- This Executor works on Python 3.7 and 3.8. 
- Make sure to install the [requirements](./requirements.txt)

## 🚀 Usages

### 🚚 Via JinaHub

#### using docker images
Use the prebuilt images from JinaHub in your python codes, 

```python
from jina import Flow
	
f = Flow().add(uses='jinahub+docker://LMDBIndexer')
```

or in the `.yml` config.
	
```yaml
jtype: Flow
pods:
  - name: encoder
    uses: 'jinahub+docker://LMDBIndexer'
```

#### using source codes
Use the source codes from JinaHub in your python codes,

```python
from jina import Flow
	
f = Flow().add(uses='jinahub://LMDBIndexer')
```

or in the `.yml` config.

```yaml
jtype: Flow
pods:
  - name: encoder
    uses: 'jinahub://LMDBIndexer'
```


### 📦️ Via Pypi

1. Install the `executor-indexers` package.

	```bash
	pip install git+https://github.com/jina-ai/executor-indexers/
	```

1. Use `executor-indexers` in your code

	```python
	from jina import Flow
	from jinahub.indexers.indexer.LMDBIndexer import LMDBIndexer
	
	f = Flow().add(uses=LMDBIndexer)
	```


### 🐳 Via Docker

1. Clone the repo and build the docker image

	```shell
	git clone https://github.com/jina-ai/executor-indexers/
	cd jinahub/indexers/indexer/LMDBIndexer
	docker build -t lmdb-image .
	```

1. Use `lmdb-image` in your codes

	```python
	from jina import Flow
	
	f = Flow().add(uses='docker://lmdb-image:latest')
	```
	

## 🎉️ Example 

```python
indexer = LMDBIndexer(
    map_size = 10485760,  
    default_traversal_paths = ['r'],
)
```

Parameters:

- `map_size`: maximum size of the database on disk
- `default_traversal_paths`: the default traversal paths for the `DocumentArray` in a request. Can be overridden with `parameters={'traversal_paths': ..}` 

Check [tests](tests/test_lmdb.py) for more usage scenarios.


### Inputs 

`Document`, with any data. It is stored in full, in bytes.

### Returns

Nothing

## 🔍️ Reference
- https://lmdb.readthedocs.io/en/release/
