# ✨ NumpyPostgresSearcher

**NumpyPostgresSearcher** is a compound Searcher Executor for Jina, made up of [NumpySearcher](../../vector/NumpySearcher) for performing similarity search on the embeddings, and of [PostgresSearcher](../../keyvalue/PostgresSearcher) for retrieving the metadata of the Documents. 

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

Additionally, you will need a running PostgreSQL database. This can be a local instance, a Docker image, or a virtual machine in the cloud. Make sure you have the credentials and connection parameters.

## 🚀 Usages

Check [integration tests](../../../../../tests/integration/psql_dump_reload) for an example on how to use it.

### 🚚 Via JinaHub

#### using docker images
Use the prebuilt images from JinaHub in your python codes, 

```python
from jina import Flow
	
f = Flow().add(uses='jinahub+docker://NumpyPostgresSearcher')
```

or in the `.yml` config.
	
```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub+docker://NumpyPostgresSearcher'
```

#### using source codes
Use the source codes from JinaHub in your code

```python
from jina import Flow
	
f = Flow().add(uses='jinahub://NumpyPostgresSearcher')
```

or in the `.yml` config.

```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub://NumpyPostgresSearcher'
```


### 📦️ Via Pypi

1. Install the `executor-indexers` package.

	```bash
	pip install git+https://github.com/jina-ai/executor-indexers/
	```

1. Use `executor-indexers` in your code

	```python
	from jina import Flow
	from jinahub.indexers.searcher.compound.NumpyPostgresSearcher import NumpyPostgresSearcher
	
	f = Flow().add(uses=NumpyPostgresSearcher)
	```


### 🐳 Via Docker

1. Clone the repo and build the docker image

	```shell
	git clone https://github.com/jina-ai/executor-indexers/
	cd jinahub/indexers/searcher/compound/NumpyPostgresSearcher
	docker build -t numpy-psql-image .
	```

1. Use `numpy-psql-image` in your codes

	```python
	from jina import Flow
	
	f = Flow().add(uses='docker://numpy-psql-image:latest')
	```
	

## 🎉️ Example 


```python
from jina import Flow, Document

f = Flow().add(uses='jinahub+docker://NumpyPostgresSearcher')

with f:
    resp = f.post(on='foo', inputs=Document(), return_results=True)
	print(f'{resp}')
```

### Inputs 

`Document` with `.embedding` the same shape as the `Documents` stored in the `NumpySearcher`. The ids of the `Documents` stored in `NumpySearcher` need to exist in the `PostgresSearcher`. Otherwise you will not get back the original metadata. 

### Returns

The NumpySearcher attaches matches to the Documents sent as inputs, with the id of the match, and its embedding.
Then, the PostgresSearcher retrieves the full metadata (original text or image blob) and attaches those to the Document.
You receive back the full Document.
