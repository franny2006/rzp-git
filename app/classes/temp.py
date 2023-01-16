import base64
import uuid
from base64 import b64encode, b64decode

strUuid = "33f7858a-53fe-482f-8527-d8c28c632b3a"
strUuid = "A89A85FD-F7B5-47F2-B3CD-7026B476F8A1"
strKonvertiert = "M/eFilP+SC+FJ9jCjGMrOg=="

print(base64.b64encode(b'strUuid'))
print(base64.standard_b64encode(b'strUuid'))
print(base64.urlsafe_b64encode(b'strUuid'))

print(base64.b64decode(strKonvertiert))


# funktioniert
bin_guid = base64.standard_b64decode(strKonvertiert)
guid = uuid.UUID(bytes=bin_guid)
print(guid)


#from b64uuid import B64UUID
#short_id = B64UUID(strUuid)
#print(short_id)
print(str(b64encode(uuid.UUID(strUuid).bytes).decode()))
print(strKonvertiert)


import pymongo
from pymongo import MongoClient
connString='mongodb://root:example@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-256'
db = 'rzp_person_perf_git'
coll = 'identitaeten'
connClient = MongoClient(connString, uuidRepresentation="standard")
connDb = connClient[db]
connColl = connDb[coll]
uuidConv = b64encode(uuid.UUID(strUuid).bytes).decode()
print(uuidConv)
result = connColl.find_one({'transaktionsId.binary.base64': uuidConv})
print(result)

result = connColl.find_one({'personId.binary.base64': uuidConv})
print(result)


from datetime import datetime
datum = '20111230'
datumF = datetime.strptime(datum, '%Y%m%d').date()
print(str(datumF))