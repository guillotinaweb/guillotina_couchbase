import logging

import couchbase.subdocument as SD
from guillotina import configure
from guillotina.catalog.catalog import DefaultSearchUtility
from guillotina_couchbase.interfaces import (ICouchbaseSearchUtility,
                                             ICouchbaseStorage)

logger = logging.getLogger(__name__)


@configure.utility(provides=ICouchbaseSearchUtility)
class SearchUtility(DefaultSearchUtility):
    async def index(self, container, datas):
        """
        {uid: <dict>}
        """
        txn = container._p_jar
        storage = txn.storage
        if not ICouchbaseStorage.providedBy(storage):
            logger.warning('Couchbase does not support multiple db types '
                           'mounted together.')
            return
        for uid, data in datas.items():
            mutations = []
            for key, value in data.items():
                mutations.append(SD.upsert(key, value))
                if len(mutations) >= 16:
                    # couchbase limit
                    await storage.bucket.mutate_in(uid, *mutations)
                    mutations = []
            if len(mutations) > 0:
                await storage.bucket.mutate_in(uid, *mutations)

    update = index
