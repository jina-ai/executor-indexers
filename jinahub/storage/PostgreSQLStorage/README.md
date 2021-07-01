# ✨ PostgreSQLIndexer

**PostgreSQLIndexer** is Indexer wrapper around the PostgreSQL DBMS. Postgres is an open source object-relational database. You can read more about it here: https://www.postgresql.org/


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
- Make sure to install the [requirements](requirements.txt)

Additionally, you will need a running PostgreSQL database. This can be a local instance, a Docker image, or a virtual machine in the cloud. Make sure you have the credentials and connection parameters. 

You can start one in a Docker container, like so: 

```bash
docker run -e POSTGRES_PASSWORD=123456  -p 127.0.0.1:5432:5432/tcp postgres:13.2 
```

## 🚀 Usages

This indexer assumes a PRIMARY KEY on the `id` field, thus you cannot add two `Document` of the same id. Make sure you clean up any existing data if you want to start fresh. 

### 🚚 Via JinaHub

#### using docker images
Use the prebuilt images from JinaHub in your python codes, 

```python
from jina import Flow
	
f = Flow().add(uses='jinahub+docker://PostgreSQLIndexer')
```

or in the `.yml` config.
	
```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub+docker://PostgreSQLIndexer'
```

#### using source codes
Use the source codes from JinaHub in your python codes,

```python
from jina import Flow
	
f = Flow().add(uses='jinahub://PostgreSQLIndexer')
```

or in the `.yml` config.

```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub://PostgreSQLIndexer'
```


### 📦️ Via Pypi

1. Install the `executor-indexers` package.

	```bash
	pip install git+https://github.com/jina-ai/EXECUTOR_REPO_NAME.git
	```

1. Use `executor-indexers` in your code

   ```python
   from jina import Flow
   from jinahub.storage.PostgreSQLStorage import PostgreSQLStorage
   
   f = Flow().add(uses=PostgreSQLStorage)
   ```


### 🐳 Via Docker

1. Clone the repo and build the docker image

	```shell
	git clone https://github.com/jina-ai/executor-indexers
	cd executor-indexers/jinahub/indexers/indexer/PostgreSQLIndexer
	docker build -t psql-indexer-image .
	```

1. Use `psql-indexer-image` in your code

	```python
	from jina import Flow
	
	f = Flow().add(uses='docker://psql-indexer-image:latest')
	```
	

## 🎉️ Example 


```python
from jina import Flow, Document

f = Flow().add(uses='jinahub://PostgreSQLIndexer')

with f:
    resp = f.post(on='/index', inputs=Document(), return_results=True)
    print(f'{resp}')
```

### Inputs 

Any type of `Document`.

### Returns

Nothing. The `Documents`s are stored.

## 🔍️ Reference

- https://www.postgresql.org/

