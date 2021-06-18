import mmap
import os
from typing import Optional, Dict

from jina import Executor, requests, DocumentArray, Document
from jina.logging.logger import JinaLogger

from jina_commons import get_logger
from jina_commons.indexers.dump import import_metas
from .file_writer import FileWriterMixin

HEADER_NONE_ENTRY = (-1, -1, -1)


class FileQueryIndexer(Executor, FileWriterMixin):
    """
    A DBMS Indexer (no query method)

    :param index_filename: the name of the file for storing the index, when not given metas.name is used.
    :param key_length: the default minimum length of the key, will be expanded one time on the first batch
    :param args:  Additional positional arguments which are just used for the parent initialization
    :param kwargs: Additional keyword arguments which are just used for the parent initialization
    """

    def __init__(
        self,
        dump_path: Optional[str] = None,
        index_filename: Optional[str] = None,
        key_length: int = 36,
        default_traversal_path='r',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dump_path = dump_path or kwargs.get('runtime_args', {}).get('dump_path')
        self.index_filename = index_filename or self.metas.name

        self.key_length = key_length
        self._size = 0

        self._start = 0
        self._page_size = mmap.ALLOCATIONGRANULARITY
        self.logger = get_logger(self)

        self.default_traversal_path = default_traversal_path

        if self.dump_path:
            self.logger.info(f'Loading dump_path {self.dump_path}')
            self._load_dump(self.dump_path)
            self.query_handler = self.get_query_handler()
            self.logger.info(f'Imported {self.size} documents.')
        else:
            self.logger.warning(
                f'No dump_path provided for {self.__class__.__name__}. Use flow.rolling_update()...'
            )
            self.query_handler = None

    @property
    def size(self) -> int:
        """
        The number of vectors or documents indexed.

        :return: size
        """
        return self._size

    def close(self):
        """Close all file-handlers and release all resources. """
        self.logger.info(f'indexer size: {self.size}')
        if self.query_handler:
            self.query_handler.close()
        super().close()

    @property
    def index_abspath(self) -> str:
        """
        Get the file path of the index storage

        :return: absolute path
        """
        os.makedirs(self.workspace, exist_ok=True)
        return os.path.join(self.workspace, self.index_filename)

    def _load_dump(self, path):
        """Load the dump at the path

        :param path: the path of the dump"""
        ids, metas = import_metas(path, str(self.runtime_args.pea_id))
        with self.get_create_handler() as write_handler:
            self._add(list(ids), list(metas), write_handler)

    @requests(on='/search')
    def search(self, docs: DocumentArray, parameters: Dict = None, **kwargs) -> None:
        """Get a document by its id

        :param docs: the documents
        :param parameters: request parameters
        :param kwargs: not used
        :return: List of the bytes of the Documents (or None, if not found)
        """
        if self.size == 0:
            self.logger.error('Searching an empty index')
            return

        if parameters is None:
            parameters = {}

        traversal_path = parameters.get('traversal_paths', self.default_traversal_path)

        for docs_array in docs.traverse(traversal_path):
            self._search(docs_array, parameters.get('is_update', True))

    def _search(self, docs: DocumentArray, is_update):
        for doc in docs:
            doc_id = doc.id
            serialized_doc = self._query([doc.id])[0]
            serialized_doc = Document(serialized_doc)
            serialized_doc.pop('content_hash')
            if is_update:
                doc.update(serialized_doc)
            else:
                doc = Document(serialized_doc, copy=True)
            doc.id = doc_id
            doc.update_content_hash()
