class KiteDBError(Exception): pass
class StorageError(KiteDBError): pass
class ValidationError(KiteDBError): pass
class TransactionError(KiteDBError): pass