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


connString='mongodb://root:example@localhost:27017/?authSource=admin&authMechanism=SCRAM-SHA-256'
db = 'rzp_rente_reza_git'
coll = 'geldleistungskonten'
connClient = MongoClient(connString, uuidRepresentation="standard")
connDb = connClient[db]
connColl = connDb[coll]
uuidConv = b64encode(uuid.UUID(strUuid).bytes).decode()
print(uuidConv)
cursor = connColl.find({})

result = connColl.find({})
#
#result = connColl.find_one({'transaktionsId': uuid.UUID("d620d31b-a922-40f2-bf76-aaa668148e7d")})
print("Warum???", result)


db = 'rzp_person_perf_git'
coll = 'identitaeten'
connClient = MongoClient(connString, uuidRepresentation="standard")
connDb = connClient[db]
connColl = connDb[coll]
transaktionsId = "e1606b08-2343-432f-aa59-9b2413a439f2"
transaktionsId = "5163b01a-4ab2-4632-bb35-f2d6ebbd2f4e"
rollen = ['RZP_BERECHTIGTER', 'RZP_ZAHLUNGSEMPFAENGER', 'RZP_MITTEILUNGSEMPFAENGER', 'RZP_KONTOINHABER']
for rolle in rollen:
    #    listResult = connColl.find_one({"rollen": rolle,  "art": "OUTBOUND", "fachlicherStatus": "FREIGEGEBEN", "transaktionsId.binary.base64": transaktionsIdConv})
    listResult = connColl.find_one({
        "rollen": rolle,
        "art": "OUTBOUND",
        "fachlicherStatus": "FREIGEGEBEN",
        "transaktionsId": uuid.UUID(transaktionsId)})

 #   print("Result", listResult)
    #print("Identität:", listResult['identitaetenId'])

    listResult = connColl.find_one({
        "transaktionsId": uuid.UUID('7fbfeb97-bff1-47f6-8f88-12ae749415af'),
        "identitaetenId": uuid.UUID('7c46e87d-823d-46c5-a59c-dd3e8651d5d2')},
                 sort=[("_id", pymongo.DESCENDING)])

#   print("Result", listResult)

sollErgebnis = "000000"
sollErgebnisI = "000000"
sollErgebnis = sollErgebnis.lstrip("0")
print("srip", sollErgebnis, sollErgebnis.strip(), int(sollErgebnisI))
