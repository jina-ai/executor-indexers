# DocCache

This is an Executor that can cache documents that it has seen before, by different combination of fields (or just one field). It then removes the Document that has the same combination of values in those fields from the DocumentArray, so it will not reach the following Executors in your Flow.

This is useful for continuously indexing Documents, and not having to worry about indexing the same Document twice. 

## Usage

See [tests](./tests)

In a Flow:

```python
with Flow(return_results=True).add(uses='cache.yml') as f:
    response = f.post(
        on='/index',
        inputs=DocumentArray(docs2),
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
