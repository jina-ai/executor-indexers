__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import os
import json
import errno
from pathlib import Path

from annoy import AnnoyIndex
from jina import Executor, Document, DocumentArray, requests


class AnnoyIndexer(Executor):
    """Annoy powered vector indexer

    For more information about the Annoy supported parameters, please consult:
        - https://github.com/spotify/annoy

    .. note::
        Annoy package dependency is only required at the query time.
    """

    ANNOY_INDEX_FILE_NAME = 'annoy.ann'
    ANNOY_INDEX_MAPPING_NAME = (
        'map.json'  # this map stores int id and docid, annoy only allows add int as id.
    )

    def __init__(
        self,
        top_k: int = 10,
        num_dim: int = 768,
        num_trees: int = 10,
        metric: str = 'euclidean',
        traverse_path: list = ['r'],
        **kwargs,
    ):
        """
        Initialize an AnnoyIndexer

        :param top_k: get tok k vectors
        :param metric: Metric can be "angular", "euclidean", "manhattan", "hamming", or "dot"
        :param num_trees: builds a forest of n_trees trees. More trees gives higher precision when querying.
        :param search_k: At query time annoy will inspect up to search_k nodes which defaults to
            n_trees * k if not provided (set to -1)
        :param args:
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.top_k = top_k
        self.metric = metric
        self.num_dim = num_dim
        self.num_trees = num_trees
        self.id_docid_map = {}
        self.request_type = None
        self.index_base_dir = f'{os.environ["JINA_WORKSPACE"]}/annoy/'
        self.index_path = self.index_base_dir + self.ANNOY_INDEX_FILE_NAME
        self.index_map_path = self.index_base_dir + self.ANNOY_INDEX_MAPPING_NAME
        self.index = AnnoyIndex(self.num_dim, 'angular')
        self.traverse_path = traverse_path
        if os.path.exists(self.index_path) and os.path.exists(self.index_map_path):
            self._load_index()

    def _load_index(self):
        self.index.load(self.index_path)
        with open(self.index_map_path, 'r') as f:
            self.id_docid_map = json.load(f)

    @requests(on='/delete')
    def delete(self, **kwargs):
        try:
            os.remove(self.index_path)
            os.remove(self.index_map_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    @requests(on='/index')
    def index(self, docs: DocumentArray, **kwargs):
        self.request_type = '/index'
        if os.path.exists(self.index_path) or os.path.exists(self.index_map_path):
            raise FileExistsError(
                'Index already exist, please remove workspace and index again.'
            )

        chunks = DocumentArray(
            list(
                filter(lambda d: d.mime_type == 'text/plain', docs.traverse_flat(self.traverse_path))
            )
        )

        for idx, doc in enumerate(chunks):
            self.id_docid_map[idx] = doc.parent_id
            self.index.add_item(idx, doc.embedding)

    @requests(on='/search')
    def search(self, docs: DocumentArray, **kwargs):
        chunks = DocumentArray(
            list(
                filter(lambda d: d.mime_type == 'text/plain', docs.traverse_flat(self.traverse_path))
            )
        )
        for doc in chunks:
            indices, dists = self.index.get_nns_by_vector(
                doc.embedding, self.top_k, include_distances=True
            )
            for idx, dist in zip(indices, dists):
                match = Document(id=self.id_docid_map[str(idx)])
                match.score.value = 1 / (1 + dist)
                doc.matches.append(match)

    def close(self):
        if self.request_type == '/index':
            self.index.build(self.num_trees)
            Path(self.index_base_dir).mkdir(parents=True, exist_ok=True)
            self.index.save(self.index_path)
            with open(self.index_map_path, 'w') as f:
                json.dump(self.id_docid_map, f)