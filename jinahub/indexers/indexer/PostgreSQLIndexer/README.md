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

Some conditions to fulfill before running the executor

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
  - name: encoder
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
  - name: encoder
    uses: 'jinahub://PostgreSQLIndexer'
```


### 📦️ Via Pypi

1. Install the `jinahub-MY-DUMMY-EXECUTOR` package.

	```bash
	pip install git+https://github.com/jina-ai/EXECUTOR_REPO_NAME.git
	```

1. Use `jinahub-MY-DUMMY-EXECUTOR` in your code

	```python
	from jina import Flow
	from jinahub.SUB_PACKAGE_NAME.MODULE_NAME import PostgreSQLIndexer
	
	f = Flow().add(uses=PostgreSQLIndexer)
	```


### 🐳 Via Docker

1. Clone the repo and build the docker image

	```shell
	git clone https://github.com/jina-ai/EXECUTOR_REPO_NAME.git
	cd EXECUTOR_REPO_NAME
	docker build -t my-dummy-executor-image .
	```

1. Use `my-dummy-executor-image` in your codes

	```python
	from jina import Flow
	
	f = Flow().add(uses='docker://my-dummy-executor-image:latest')
	```
	

## 🎉️ Example 


```python
from jina import Flow, Document

f = Flow().add(uses='jinahub+docker://PostgreSQLIndexer')

with f:
    resp = f.post(on='foo', inputs=Document(), return_resutls=True)
	print(f'{resp}')
```

### Inputs 

`Document` with `blob` of the shape `256`.

### Returns

`Document` with `embedding` fields filled with an `ndarray` of the shape `embedding_dim` (=128, by default) with `dtype=nfloat32`.


## 🔍️ Reference

- https://www.postgresql.org/

