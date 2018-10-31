import asyncio


async def test_install(guillotina_couchbase_requester):  # noqa
    async with guillotina_couchbase_requester as requester:
        response, _ = await requester('GET', '/db/guillotina/@addons')
        assert 'guillotina_couchbase' in response['installed']
