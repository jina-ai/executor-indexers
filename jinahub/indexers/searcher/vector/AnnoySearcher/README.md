# ✨ AnnoySearcher

**AnnoySearcher** is an Annoy-powered vector-based similarity searcher. Annoy stands for "Approximate Nearest Neighbors Oh Yeah", and is a C++ library with Python bindings to search for points in space that are close to a given query point. It also creates large read-only file-based data structures that are mmapped into memory so that many processes may share the same data.

For more information, refer to the GitHub repo for [Spotify's Annoy](https://github.com/spotify/annoy).

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

Check [tests](./tests) for an example on how to use it.

### 🚚 Via JinaHub

#### using docker images
Use the prebuilt images from JinaHub in your python codes, 

```python
from jina import Flow
	
f = Flow().add(uses='jinahub+docker://AnnoySearcher')
```

or in the `.yml` config.
	
```yaml
jtype: Flow
pods:
  - name: encoder
    uses: 'jinahub+docker://AnnoySearcher'
```

#### using source codes
Use the source codes from JinaHub in your code

```python
from jina import Flow
	
f = Flow().add(uses='jinahub://AnnoySearcher')
```

or in the `.yml` config.

```yaml
jtype: Flow
pods:
  - name: encoder
    uses: 'jinahub://AnnoySearcher'
```


### 📦️ Via Pypi

1. Install the `executor-indexers` package.

	```bash
	pip install git+https://github.com/jina-ai/executor-indexers/
	```

1. Use `executor-indexers` in your code

	```python
	from jina import Flow
	from jinahub.indexers.searcher.vector.AnnoySearcher import AnnoySearcher
	
	f = Flow().add(uses=AnnoySearcher)
	```


### 🐳 Via Docker

1. Clone the repo and build the docker image

	```shell
	git clone https://github.com/jina-ai/executor-indexers/
	cd jinahub/indexers/searcher/vector/AnnoySearcher
	docker build -t annoy-image .
	```

1. Use `annoy-image` in your codes

	```python
	from jina import Flow
	
	f = Flow().add(uses='docker://annoy-image:latest')
	```
	

## 🎉️ Example 


```python
from jina import Flow, Document

f = Flow().add(uses='jinahub+docker://AnnoySearcher')

with f:
    resp = f.post(on='foo', inputs=Document(), return_results=True)
	print(f'{resp}')
```

### Inputs 

`Document` with `.embedding` the same shape as the `Documents` it has stored.

### Returns

Attaches matches to the Documents sent as inputs, with the id of the match, and its embedding. For retrieving the full metadata (original text or image blob), use a [key-value searcher](./../../keyvalue).


## 🔍️ Reference

- https://github.com/spotify/annoy
