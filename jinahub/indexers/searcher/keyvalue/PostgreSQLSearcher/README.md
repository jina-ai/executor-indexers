# ✨ PostgreSQLSearcher

**PostgreSQLSearcher** is a Searcher-type Executor for Jina. It searches your PostgreSQL database by the given ids of your `Documents`, and then updates them with the given metadata content.

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

### 🚚 Via JinaHub

#### using docker images
Use the prebuilt images from JinaHub in your python codes, 

```python
from jina import Flow
	
f = Flow().add(uses='jinahub+docker://PostgreSQLSearcher')
```

or in the `.yml` config.
	
```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub+docker://PostgreSQLSearcher'
```

#### using source codes
Use the source codes from JinaHub in your python codes,

```python
from jina import Flow
	
f = Flow().add(uses='jinahub://PostgreSQLSearcher')
```

or in the `.yml` config.

```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub://PostgreSQLSearcher'
```


### 📦️ Via Pypi

1. Install the `jinahub-MY-DUMMY-EXECUTOR` package.

	```bash
	pip install git+https://github.com/jina-ai/EXECUTOR_REPO_NAME.git
	```

1. Use `jinahub-MY-DUMMY-EXECUTOR` in your code

	```python
	from jina import Flow
	from jinahub.SUB_PACKAGE_NAME.MODULE_NAME import PostgreSQLSearcher
	
	f = Flow().add(uses=PostgreSQLSearcher)
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

f = Flow().add(uses='jinahub+docker://PostgreSQLSearcher')

with f:
    resp = f.post(on='foo', inputs=Document(), return_results=True)
	print(f'{resp}')
```

### Inputs 

`Document` with `blob` of the shape `256`.

### Returns

`Document` with `embedding` fields filled with an `ndarray` of the shape `embedding_dim` (=128, by default) with `dtype=nfloat32`.


## 🔍️ Reference
- Some reference

