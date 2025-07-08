import backoff
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from loguru import logger

from palver_elastic_client.backoff import backoff_hdlr
from palver_elastic_client.util.dict import omit


class Elastico:
    client: AsyncElasticsearch

    def __init__(self, url: str, token: str):
        logger.info(f"Connecting to Elasticsearch at {url}, with token {token}")

        self.client = AsyncElasticsearch(
            url,
            api_key=token,
            max_retries=3,
            retry_on_timeout=True,
        )

    async def _produce_bulk_update_iterator(
        self, index: str, data: list[dict], id_key: str = "id"
    ):
        for item in data:
            yield {
                "_op_type": "update",
                "_index": index,
                "_id": item[id_key],
                "doc": omit(item, [id_key]),
            }

    async def _produce_bulk_insert_iterator(self, index: str, data: list[dict]):
        for item in data:
            yield {
                "_op_type": "index",
                "_index": index,
                "_id": item["id"],
                "_source": item,
            }

    async def ping(self) -> bool:
        return await self.client.ping()

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=2,
        on_backoff=backoff_hdlr,
    )
    async def bulk_update(
        self, index: str, data: list[dict], id_key: str = "id"
    ) -> dict:
        logger.debug(
            f"Updating {len(data)} items in index {index}, matching on {id_key}"
        )

        try:
            return await async_bulk(
                self.client,
                actions=self._produce_bulk_update_iterator(index, data, id_key),
            )
        except Exception as e:
            logger.error(f"Error updating index {index}: {e}")
            raise e

    async def bulk_insert(self, index: str, data: list[dict]) -> dict:
        logger.debug(f"Inserting {len(data)} items in index {index}")

        try:
            return await async_bulk(
                self.client,
                actions=self._produce_bulk_insert_iterator(index, data),
            )
        except Exception as e:
            logger.error(f"Error updating index {index}: {e}")
            raise e

    async def bulk_delete_by_query(self, index: str, query: dict) -> dict:
        logger.debug(f"Deleting documents from index {index} using query: {query}")

        try:
            response = await self.client.delete_by_query(
                index=index,
                query=query,
                conflicts="proceed",
                refresh=True,
                wait_for_completion=True,
            )
            logger.info(f"Deleted {response.get('deleted', 0)} documents from {index}")
            return response
        except Exception as e:
            logger.error(f"Error in delete_by_query from index {index}: {e}")
            raise e

    async def _produce_bulk_upsert_iterator(
        self, index: str, data: list[dict], id_key: str = "id"
    ):
        for item in data:
            yield {
                "_op_type": "update",
                "_index": index,
                "_id": item[id_key],
                "doc": item,
                "doc_as_upsert": True,
            }

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=2,
        on_backoff=backoff_hdlr,
    )
    async def bulk_upsert(
        self, index: str, data: list[dict], id_key: str = "id"
    ) -> dict:
        logger.debug(
            f"Upserting {len(data)} items in index {index}, matching on {id_key}"
        )

        try:
            return await async_bulk(
                self.client,
                actions=self._produce_bulk_upsert_iterator(index, data, id_key),
            )
        except Exception as e:
            logger.error(f"Error upserting into index {index}: {e}")
            raise e
