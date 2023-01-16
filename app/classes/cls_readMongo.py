from pymongo import MongoClient
import pymongo
import uuid
import json
import bson
import bson.json_util as json_util
from django.core.serializers.json import DjangoJSONEncoder
from bson.codec_options import CodecOptions
from bson.binary import UuidRepresentation
from base64 import b64encode, b64decode

from .cls_db import cls_dbAktionen

class cls_readMongo():
    def __init__(self):
        with open('config/config.json') as json_file:
            self.configData = json.load(json_file)

    def readTransaktionsId(self, panr, prnr, voat, lfdNr):
   #     print(self.configData)
        connString = ""
        db = ""
        collection = ""
        for connection in self.configData['connection']:
            if connection['connectionName'].lower() == "transaktionsid":
                connString = connection['connectionString']
                db = connection['database']
                collection = connection['collection']

        connClient = MongoClient(connString)
        connDb = connClient[db]
        connColl = connDb[collection]

        listResults = connColl.find_one({'$and': [
            {'keyValue.allgemeinerTeil.prnr': prnr},
            {'keyValue.allgemeinerTeil.voat': voat},
            {'keyValue.allgemeinerTeil.laufendeNummerZL': lfdNr},
            {'panr': panr}
        ]})


        statusAnliegen = {'_id': '', 'pruefergebnis': ''}
        anzHinweise = 0
        listHinweise = [{'hinweisCode': '', 'hinweisText': ''}]
        anzFehler = 0
        listFehler = [{'fehlerCode': '', 'fehlerText': ''}]

        if listResults:
            if listResults['fehler']:
                listFehler = []
                anzFehler=len(listResults['fehler'])
                for fehler in listResults['fehler']:
                    dictFehler = {}
                    dictFehler['fehlerCode'] = fehler['empfaengercode']
                    dictFehler['fehlerText'] = fehler['bezeichner']
                    listFehler.append(dictFehler)
            if listResults['hinweise']:
                listHinweise = []
                anzHinweise=len(listResults['hinweise'])
                for hinweis in listResults['hinweise']:
                    dictHinweise = {}
                    dictHinweise['hinweisCode'] = hinweis['empfaengercode']
                    dictHinweise['hinweisText'] = hinweis['bezeichner']
                    listHinweise.append(dictHinweise)
            statusAnliegen = {
                'panr': panr,
                'prnr': prnr,
                'voat': voat,
                'lfdNr': lfdNr,
                'transaktionsId': listResults['_id'],
                'pruefergebnis': listResults['pruefergebnis'],
                'anzHinweise': anzHinweise,
                'hinweis1': listHinweise[0]['hinweisCode'],
                'anzFehler': anzFehler,
                'fehler1': listFehler[0]['fehlerCode']}


            # Speichern des Dokuments
       #     print(listResults)
            self.saveDocument("KUGA", listResults['_id'], None, None, None, listResults)

        self.writeTransaktionsId(statusAnliegen)
        # Speichern der TransaktionsId
        self.writeStatusApp("KUGA", "1", listResults['_id'])


        return statusAnliegen

    def writeTransaktionsId(self, statusAnliegen):
        self.db = cls_dbAktionen()
        sql_valuelist = [str(statusAnliegen['panr']), str(statusAnliegen['prnr']), str(statusAnliegen['voat']), str(statusAnliegen['lfdNr']),
                         str(statusAnliegen['transaktionsId']), str(statusAnliegen['pruefergebnis']), str(statusAnliegen['anzHinweise']), str(statusAnliegen['hinweis1']), str(statusAnliegen['anzFehler']), str(statusAnliegen['fehler1'])]
        sql = "insert into transaktionIds (panr, prnr, voat, lfdNr, transaktionsid, pruefergebnis, anzHinweise, hinweis, anzFehler, fehler) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
              "ON DUPLICATE KEY UPDATE " \
              "transaktionsid='" + str(statusAnliegen['transaktionsId']) + "', " \
              "pruefergebnis='" + str(statusAnliegen['pruefergebnis']) + "', " \
              "anzHinweise='" + str(statusAnliegen['anzHinweise']) + "', " \
              "hinweis='" + str(statusAnliegen['hinweis1']) + "', " \
              "anzFehler='" + str(statusAnliegen['anzFehler']) + "', " \
              "fehler='" + str(statusAnliegen['fehler1']) + "'"
        # print("Schreiben der TransaktionsId: ", sql, sql_valuelist)
        self.db.execSql(sql, sql_valuelist)

    def readRollenIds(self, transaktionsId):
        rollen = ['RZP_BERECHTIGTER', 'RZP_ZAHLUNGSEMPFAENGER', 'RZP_MITTEILUNGSEMPFAENGER', 'RZP_KONTOINHABER']
        connInfos = self.readConnectionInfos('PERF_IDENT')
        connClient = MongoClient(connInfos['connString'], uuidRepresentation="standard")
        opts = CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY)
        connDb = connClient[connInfos['db']]
        connColl = connDb[connInfos['collection']]
        transaktionsIdConv = b64encode(uuid.UUID(transaktionsId).bytes).decode()
        rollenList = []
        for rolle in rollen:
            listResult = connColl.find_one({"rollen": rolle,  "art": "OUTBOUND", "fachlicherStatus": "FREIGEGEBEN", "transaktionsId.binary.base64": transaktionsIdConv})
            if listResult:
                identitaetenId = listResult['identitaetenId']['binary']['base64']
                personId = listResult['personId']['binary']['base64']
                if rolle == "RZP_BERECHTIGTER":
                    postfix = "be"
                elif rolle == "RZP_ZAHLUNGSEMPFAENGER":
                    postfix = "ze"
                elif rolle == "RZP_MITTEILUNGSEMPFAENGER":
                    postfix = "me"
                elif rolle == "RZP_KONTOINHABER":
                    postfix = "ki"
                sqlUpdateRollen = "update transaktionIds set identitaetenId_" + str(postfix) + " = '" + str(identitaetenId) + "', personId_" + str(postfix) + " = '" + str(personId) + "'"
                self.db.execSql(sqlUpdateRollen, '')
                rolleDict = {
                    'rolle': postfix,
                    'identitaetenId': str(identitaetenId),
                    'personId': str(personId)
                }
                rollenList.append(rolleDict)

        return rollenList



    def readApplication(self, transaktionsId, appName, nameTransaktionsId, rolle, nameId2, id2, nameSortfield):
        connInfos = self.readConnectionInfos(appName)

        connClient = MongoClient(connInfos['connString'], uuidRepresentation="standard")
        opts = CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY)

        connDb = connClient[connInfos['db']]
        connColl = connDb[connInfos['collection']]

        if appName in ("AAN"):
            result = connColl.find_one(
                {nameTransaktionsId: transaktionsId},
                 sort=[(nameSortfield, pymongo.DESCENDING)])
        elif appName in ("PERF_IDENT", "PERF_PERS"):
            transaktionsIdConv = b64encode(uuid.UUID(transaktionsId).bytes).decode()
            result = connColl.find_one(
                {nameTransaktionsId: transaktionsIdConv,
                 nameId2: id2},
                 sort=[(nameSortfield, pymongo.DESCENDING)])
        else:
            result = connColl.find_one(
                {nameTransaktionsId: UUID(transaktionsId)},
                 sort=[(nameSortfield, pymongo.DESCENDING)])

        if result:
            # Speichern des Dokuments
            self.saveDocument(appName, transaktionsId, rolle, nameId2, id2, result)

            # Speichern des Status, wenn Transaktion in KAUS gefunden wurde
            self.writeStatusApp(appName, "1", transaktionsId)
        else:
            self.writeStatusApp(appName, "2", transaktionsId)


    def readKaus(self, transaktionsId):
        connInfos = self.readConnectionInfos("KAUS")

        connClient = MongoClient(connInfos['connString'], uuidRepresentation="standard")
        opts = CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY)

        connDb = connClient[connInfos['db']]
        connColl = connDb[connInfos['collection']]

        result = connColl.find_one(
            {'transaktionsId': UUID(transaktionsId)},
            sort=[('historienstufe', pymongo.DESCENDING)])

        if result:
            # Speichern des Status, wenn Transaktion in KAUS gefunden wurde
            self.writeStatusApp("KAUS", "1", transaktionsId)
            self.writeKaus(transaktionsId, result)

            # Speichern des Dokuments
            self.saveDocument("KAUS", transaktionsId, None, None, None, result)
        else:
            self.writeStatusApp("KAUS", "2", transaktionsId)



    def writeKaus(self, transaktionsId, result):
        self.db = cls_dbAktionen()
        # Schreibe Rollen
        rollen = ['berechtigter', 'schreibenempfaenger', 'zahlungsempfaenger']
        for rolle in rollen:
            if rolle in result.keys():
                art = rolle
                id = str(result[art][rolle+'Id'])
                try:
                    abweichend = result[art]['abweichend']
                except:
                    abweichend = "--"
                anrede = result[art]['name']['anrede']
                zuname = result[art]['name']['nachname']
                vorname = result[art]['name']['vorname']
                if rolle != "zahlungsempfaenger":
                    strasse = result[art]['adresse']['strasse']
                    hausnummer = result[art]['adresse']['hausnummer']
                    plz = result[art]['adresse']['plz']
                    ort = result[art]['adresse']['ort']
                    land = result[art]['adresse']['wohnsitzland']
                    kommunikationsmerkmal = result[art]['kommunikationsmerkmal']
                else:
                    strasse = ""
                    hausnummer = ""
                    plz = ""
                    ort = ""
                    land = ""
                    kommunikationsmerkmal = ""
                try:
                    geburtsdatum = result[art]['geburtsdatum']
                except:
                    geburtsdatum = "--"


                sql_valuelist = [transaktionsId, id, art, anrede, zuname, vorname, strasse, hausnummer, plz, ort, land, geburtsdatum, kommunikationsmerkmal]


                sql = "insert into rollen (transaktionsid, id, art, anrede, zuname, vorname, strasse, hausnummer, plz, ort, land, geburtsdatum, kommunikationsmerkmal) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
                      "ON DUPLICATE KEY UPDATE " \
                      "abweichend='" + str(abweichend) + "', " \
                      "anrede='" + str(anrede) + "', " \
                      "zuname='" + str(zuname) + "', " \
                      "vorname='" + str(vorname) + "', " \
                      "strasse='" + str(strasse) + "', " \
                      "hausnummer='" + str(hausnummer) + "', " \
                      "plz='" + str(plz) + "', " \
                      "ort='" + str(ort) + "', " \
                      "land='" + str(land) + "', " \
                      "kommunikationsmerkmal='" + str(kommunikationsmerkmal) + "', " \
                      "geburtsdatum='" + str(geburtsdatum) + "'"

            #    self.db.execSql(sql, sql_valuelist)

    def saveDocument(self, herkunft, transaktionsId, rolle, nameId2, id2, result):
        # Speichern des Dokuments
        self.db = cls_dbAktionen()
        document = json.dumps(result, cls=myEncoder, ensure_ascii=False)
        if nameId2 is None:
            sql_writeDokument = "insert into documents (herkunft, transaktionsId, document) values ('" + herkunft + "', '" + transaktionsId + "', '" + document + "') " \
            "ON DUPLICATE KEY UPDATE document = '" + document + "'"
        else:
            if herkunft == "PERF_IDENT":
                art = "identitaetenId"
            elif herkunft == "PERF_PERS":
                art = "personId"

            sql_writeDokument = "insert into documents (herkunft, transaktionsId, rolle, " + art + ", document) values ('" + herkunft + "', '" + transaktionsId + "', '" + rolle + "', '" + id2 + "', '" + document + "') " \
            "ON DUPLICATE KEY UPDATE document = '" + document + "'"
        print(sql_writeDokument)
        self.db.execSql(sql_writeDokument, '')

    def writeStatusApp(self, appName, status, transaktionsId):
        self.db = cls_dbAktionen()
        sql = "update transaktionIds set " + str(appName).lower() + " = '" + status + "' where transaktionsId = '" + transaktionsId + "'"
     #   print("Update Status", sql)
        self.db.execSql(sql, '')


    def readConnectionInfos(self, database):
        connInfos = {}
        for connection in self.configData['connection']:
            if connection['connectionName'].lower() == database.lower():
                connInfos['connString'] = connection['connectionString']
                connInfos['db'] = connection['database']
                connInfos['collection'] = connection['collection']
        return connInfos


from uuid import UUID
class myEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        if isinstance(obj, object):
            # if the obj is uuid, we simply return the value of uuid
            return str(obj)
        return json.JSONEncoder.default(self, obj)