# ✨ DocCache

DocCache is an Executor that can cache documents that it has seen before, by different combination of fields (or just one field). It then removes the Document that has the same combination of values in those fields from the DocumentArray, so it will not reach the following Executors in your Flow. 

This is useful for continuously indexing Documents, and not having to worry about indexing the same Document twice.

## Notes

The Executor only removes Documents in the `/index` endpoint. In the other endpoints, operations are done by the Document `id`.

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
	
f = Flow().add(uses='jinahub+docker://DocCache')
```

or in the `.yml` config.
	
```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub+docker://DocCache'
```

#### using source codes
Use the source codes from JinaHub in your python codes,

```python
from jina import Flow
	
f = Flow().add(uses='jinahub://DocCache')
```

or in the `.yml` config.

```yaml
jtype: Flow
pods:
  - name: indexer
    uses: 'jinahub://DocCache'
```


### 📦️ Via Pypi

1. Install the `executor-indexers` package.

	```bash
	pip install git+https://github.com/jina-ai/executor-indexers/
	```

1. Use `executor-indexers` in your code

	```python
	from jina import Flow
	from jinahub.indexers.cache import DocCache
	
	f = Flow().add(uses=DocCache)
	```


### 🐳 Via Docker

1. Clone the repo and build the docker image

	```shell
	git clone https://github.com/jina-ai/executor-indexers/
	cd jinahub/indexers/cache
	docker build -t doc-cache-image .
	```

1. Use `doc-cache-image` in your code

	```python
	from jina import Flow
	
	f = Flow().add(uses='docker://doc-cache-image:latest')
	```
	

## 🎉️ Example 

See [tests](./tests)

In a Flow:

```python
with Flow(return_results=True).add(uses='cache.yml') as f:
    response = f.post(
        on='/index',
        inputs=DocumentArray(docs),
    )
```


with your `cache.yaml` being:

```yaml
jtype: DocCache
with:
  fields: $CACHE_FIELDS
metas:
  name: cache
  workspace: $CACHE_WORKSPACE
```

You can replace `$CACHE_FIELDS` with either one string (e.g. `text`), or with a list (e.g. `[text, tags__author]`).

