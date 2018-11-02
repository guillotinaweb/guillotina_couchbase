from guillotina.db.interfaces import IStorage
from guillotina.interfaces import ICatalogUtility


class ICouchbaseStorage(IStorage):
    pass


class ICouchbaseSearchUtility(ICatalogUtility):
    pass
