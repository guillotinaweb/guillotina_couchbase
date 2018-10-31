import base64
import logging
import os
import uuid

import couchbase.exceptions
import couchbase.experimental
from couchbase.n1ql import N1QLQuery
from guillotina import configure
from guillotina.db import ROOT_ID
from guillotina.db.storages.base import BaseStorage
from guillotina.factory.content import Database
from guillotina.interfaces import IDatabaseConfigurationFactory
from guillotina_couchbase.interfaces import ICouchbaseStorage
from zope.interface import implementer

couchbase.experimental.enable()

logger = logging.getLogger('guillotina')


@configure.utility(provides=IDatabaseConfigurationFactory, name="couchbase")
async def CouchbaseDatabaseConfigurationFactory(key, dbconfig, loop=None):
    dss = CouchbaseStorage(**dbconfig)
    if loop is not None:
        await dss.initialize(loop=loop)
    else:
        await dss.initialize()

    db = Database(key, dss)
    await db.initialize()
    return db


@implementer(ICouchbaseStorage)
class CouchbaseStorage(BaseStorage):
    """
    Dummy in-memory storage for testing
    """

    _transaction_strategy = 'resolve'
    _supports_unique_constraints = True

    _indexes_fields = (
        'zoid', 'id', 'part', 'resource', 'of',
        'parent_id', 'type', 'otid', 'tid')
    _create_statement = 'CREATE INDEX {bucket}_object_{field} ON `{bucket}`({field})'  # noqa

    def __init__(self, read_only=False, dsn=None, username=None,
                 password=None, bucket=None, **kwargs):
        self._dsn = dsn
        self._username = username
        self._password = password
        self._bucket = bucket
        self._cb = None
        super().__init__(read_only)

    async def finalize(self):
        pass

    async def initialize(self, loop=None):
        from acouchbase.bucket import Bucket

        self._cb = Bucket(
            os.path.join(self._dsn, self._bucket),
            username=self._username, password=self._password)
        await self._cb.connect()

        installed_indexes = []
        primary_installed = False
        async for row in self._cb.n1ql_query(
                N1QLQuery('select * from system:indexes')):
            if row['indexes']['namespace_id'] != self._bucket:
                continue
            if row['indexes'].get('is_primary'):
                primary_installed = True
            else:
                installed_indexes.append(
                    row['indexes']['index_key'][0].strip('`'))

        if not primary_installed:
            async for row in self._cb.n1ql_query(  # noqa
                    'CREATE PRIMARY INDEX ON {bucket}'.format(
                        bucket=self._bucket)):
                pass

        for field in self._indexes_fields:
            if field in installed_indexes:
                continue
            statement = self._create_statement.format(
                bucket=self._bucket, field=field)
            async for row in self._cb.n1ql_query(  # noqa
                    statement.format(bucket=self._bucket)):
                pass

    async def remove(self):
        """Reset the tables"""
        pass

    async def open(self):
        return self

    async def close(self, con):
        pass

    async def root(self):
        return await self.load(None, ROOT_ID)

    async def last_transaction(self, txn):
        return self._last_transaction

    async def get_next_tid(self, txn):
        if txn._tid is None:
            txn._tid = str(uuid.uuid4())
        return txn._tid

    async def load(self, txn, oid):
        try:
            result = await self._cb.get(oid)
            value = result.value
            value['state'] = base64.b64decode(value['state'])
            return value
        except couchbase.exceptions.NotFoundError:
            raise KeyError(oid)

    async def start_transaction(self, txn):
        pass

    def get_txn(self, txn):
        if not getattr(txn, '_db_txn', None):
            txn._db_txn = {
                'added': {},
                'removed': []
            }
        return txn._db_txn

    async def store(self, oid, old_serial, writer, obj, txn):
        p = writer.serialize()  # This calls __getstate__ of obj
        json = await writer.get_json()
        part = writer.part
        if part is None:
            part = 0

        await self._cb.upsert(oid, {
            'tid': await self.get_next_tid(txn),
            'zoid': oid,
            'size': len(p),
            'part': part,
            'resource': writer.resource,
            'of': writer.of,
            'otid': old_serial,
            'parent_id': writer.parent_id,
            'id': writer.id,
            'type': writer.type,
            'json': json,
            'state': base64.b64encode(p).decode('ascii')
        })
        return 0, len(p)

    async def delete(self, txn, oid):
        await self._cb.delete(oid)

    async def commit(self, transaction):
        return await self.get_next_tid(transaction)

    async def abort(self, transaction):
        transaction._db_txn = None

    async def keys(self, txn, oid):
        keys = []
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT id from `{}`
WHERE parent_id = $1'''.format(self._bucket), oid)):
            keys.append(row)
        return keys

    async def get_child(self, txn, parent_id, id):
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT zoid, tid, state_size, resource, type, state, id
FROM `{}`
WHERE parent_id = $1 AND id = $2
'''.format(self._bucket), parent_id, id)):
            row['state'] = base64.b64decode(row['state'])
            return row

    async def has_key(self, txn, parent_id, id):
        async for row in self._cb.n1ql_query(  # noqa
                N1QLQuery('''
SELECT zoid
FROM `{}`
WHERE parent_id = $1 AND id = $2
'''.format(self._bucket), parent_id, id)):
            return True
        return False

    async def len(self, txn, oid):
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT count(*) FROM `{}` WHERE parent_id = $1
'''.format(self._bucket), oid)):
            return row['$1']
        return 0

    async def items(self, txn, oid):  # pragma: no cover
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT zoid, tid, state_size, resource, type, state, id
FROM `{}`
WHERE parent_id = $1
'''.format(self._bucket), oid)):
            row['state'] = base64.b64decode(row['state'])
            yield row

    async def get_children(self, txn, parent, keys):
        items = []
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT zoid, tid, state_size, resource, type, state, id
FROM `{}`
WHERE parent_id = $1 AND id IN $2
'''.format(self._bucket), parent, keys)):
            row['state'] = base64.b64decode(row['state'])
            items.append(row)
        return items

    async def get_annotation(self, txn, oid, id):
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT zoid, tid, state_size, resource, type, state, id, parent_id
FROM `{}`
WHERE
    of = $1 AND id = $2
'''.format(self._bucket), oid, id)):
            row['state'] = base64.b64decode(row['state'])
            return row

    async def get_annotation_keys(self, txn, oid):
        async for row in self._cb.n1ql_query(
                N1QLQuery('''
SELECT id, parent_id
FROM `{}`
WHERE of = $1
'''.format(self._bucket), oid)):
            return row

    async def del_blob(self, txn, bid):
        raise NotImplementedError()

    async def write_blob_chunk(self, txn, bid, oid, chunk_index, data):
        raise NotImplementedError()

    async def read_blob_chunk(self, txn, bid, chunk=0):
        raise NotImplementedError()

    async def get_conflicts(self, txn):
        return []

    async def get_page_of_keys(self, txn, oid, page=1, page_size=1000):
        print('get_page_of_keys {} {}'.format(oid, id))
