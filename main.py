from palver_elastic_client.elastico import Elastico


async def main():
    elastico = Elastico(
        url="config.elastic_base_url",
        token="config.elastic_token",
    )

    await elastico.ping()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
