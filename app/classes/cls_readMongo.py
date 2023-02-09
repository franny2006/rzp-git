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
from configparser import ConfigParser

from .cls_db import cls_dbAktionen
from .cls_readAuftraege import cls_readAuftraege

class cls_readMongo():
    def __init__(self):
        config = ConfigParser()
        config.read('config/config.ini')
        if config.get('Installation', 'zielumgebung').lower() == "local":
            ziel = 'mongo-db-config local'
        else:
            ziel = 'mongo-db-config docker'
        with open(config.get(ziel, 'configfile')) as json_file:
            self.configData = json.load(json_file)

    def readTransaktionsIds(self, listAuftraege):
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
        readAuftraege = cls_readAuftraege()

        for auftrag in listAuftraege:
            dictAuftragUnique = readAuftraege.read_Auftrag_Unique(str(auftrag['runId']), str(auftrag['dsId']))

            sendungsnummer = auftrag['sendungsnummer']
            datei = auftrag['datei']
            panr = dictAuftragUnique[0]['panr']
            prnr = dictAuftragUnique[0]['prnr']
            voat = dictAuftragUnique[0]['voat']
            lfdNr = dictAuftragUnique[0]['lfdNr']

            # Eindeutige LieferungsId passend zur Sendungsnummer ermitteln
            connColl = connDb["lieferung"]
            listResultsLieferungen = connColl.find_one({
                '$and': [
                    {
                        'sendungsnummer': sendungsnummer},
                    {
                        'dateiname': datei}
                ]}, sort=[('_id', pymongo.DESCENDING)])

            lieferungsId = ""
            if listResultsLieferungen:
                lieferungsId = listResultsLieferungen['_id']

            # Transaktionsnummer aus Collection "anliegen" zur LieferungsId ermitteln
            connColl = connDb[collection]

            listResults = connColl.find_one({
                '$and': [
                    {
                        'lieferungUuid': lieferungsId},
                    {
                        'keyValue.allgemeinerTeil.prnr': prnr},
                    {
                        'keyValue.allgemeinerTeil.voat': voat},
                    {
                        'keyValue.allgemeinerTeil.laufendeNummerZL': lfdNr},
                    {
                        'panr': panr}
                ]}, sort=[('_id', pymongo.DESCENDING)])

            statusAnliegen = {
                '_id': '',
                'pruefergebnis': '',
                'transaktionsId': 'None'}
            anzHinweise = 0
            listHinweise = [{
                                'hinweisCode': '',
                                'hinweisText': ''}]
            anzFehler = 0
            listFehler = [{
                              'fehlerCode': '',
                              'fehlerText': ''}]

            if listResults:
                if listResults['fehler']:
                    listFehler = []
                    anzFehler = len(listResults['fehler'])
                    for fehler in listResults['fehler']:
                        dictFehler = {}
                        dictFehler['fehlerCode'] = fehler['empfaengercode']
                        dictFehler['fehlerText'] = fehler['bezeichner']
                        listFehler.append(dictFehler)
                if listResults['hinweise']:
                    listHinweise = []
                    anzHinweise = len(listResults['hinweise'])
                    for hinweis in listResults['hinweise']:
                        dictHinweise = {}
                        dictHinweise['hinweisCode'] = hinweis['empfaengercode']
                        dictHinweise['hinweisText'] = hinweis['bezeichner']
                        listHinweise.append(dictHinweise)
                statusAnliegen = {
                    'datei': datei,
                    'sendungsnummer': sendungsnummer,
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
                self.writeStatusApp("KUGA", "1", listResults['_id'], '')



    def writeTransaktionsId(self, statusAnliegen):
        self.db = cls_dbAktionen()

        sql_valuelist = [str(statusAnliegen['datei']), str(statusAnliegen['sendungsnummer']), str(statusAnliegen['panr']), str(statusAnliegen['prnr']), str(statusAnliegen['voat']), str(statusAnliegen['lfdNr']),
                         str(statusAnliegen['transaktionsId']), str(statusAnliegen['pruefergebnis']), str(statusAnliegen['anzHinweise']), str(statusAnliegen['hinweis1']), str(statusAnliegen['anzFehler']), str(statusAnliegen['fehler1'])]
        sql = "insert into transaktionIds (datei, sendungsnummer, panr, prnr, voat, lfdNr, transaktionsid, pruefergebnis, anzHinweise, hinweis, anzFehler, fehler) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
              "ON DUPLICATE KEY UPDATE " \
              "transaktionsid='" + str(statusAnliegen['transaktionsId']) + "', " \
              "pruefergebnis='" + str(statusAnliegen['pruefergebnis']) + "', " \
              "anzHinweise='" + str(statusAnliegen['anzHinweise']) + "', " \
              "hinweis='" + str(statusAnliegen['hinweis1']) + "', " \
              "anzFehler='" + str(statusAnliegen['anzFehler']) + "', " \
              "fehler='" + str(statusAnliegen['fehler1']) + "'"
      #  print("Schreiben der TransaktionsId: ", sql, sql_valuelist)
        self.db.execSql(sql, sql_valuelist)

    def readApplication(self, appName, listAuftraege, id2, rolle):
        dbInfos = self.readDbInfos(appName)

        connInfos = self.readConnectionInfos(appName)
        connClient = MongoClient(connInfos['connString'], uuidRepresentation="standard")
        opts = CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY)
        connDb = connClient[connInfos['db']]
        connColl = connDb[connInfos['collection']]

        for auftrag in listAuftraege:
            if auftrag['transaktionsId'] != None:

                if appName in ("AAN"):
                    result = connColl.find_one(
                        {
                            dbInfos['feldIdentifizierung_1']: auftrag['transaktionsId']},
                        sort=[(dbInfos['feldSortierung'], pymongo.DESCENDING)])



                elif appName in ("PERF_IDENT", "PERF_PERS"):
                    if appName == "PERF_IDENT":
                        id2 = 'identitaetenId_' + str(rolle)
                    else:
                        id2 = 'personId_' + str(rolle)

                    if auftrag[id2] != None:
                        transaktionsIdConv = b64encode(uuid.UUID(auftrag['transaktionsId']).bytes).decode()
                        result = connColl.find_one(
                            {
                                dbInfos['feldIdentifizierung_1']: UUID(auftrag['transaktionsId']),
                                dbInfos['feldIdentifizierung_2']: UUID(auftrag[id2])},
                            sort=[(dbInfos['feldSortierung'], pymongo.DESCENDING)])
                    else:
                        result = None

                else:  # KAUS, WCH, REZA, REFUE
                    result = connColl.find_one(
                        {
                            dbInfos['feldIdentifizierung_1']: UUID(auftrag['transaktionsId'])},
                        sort=[(dbInfos['feldSortierung'], pymongo.DESCENDING)])

                if result:
                    # Speichern des Dokuments
                    self.saveDocument(appName, auftrag['transaktionsId'], rolle, dbInfos['feldIdentifizierung_2'], id2, result)
                    statusDok = self.readStatus(appName, auftrag['transaktionsId'], dbInfos['feldStatus'], result)

                    # Speichern des Status, wenn Transaktion gefunden wurde
                    self.writeStatusApp(appName, "1", auftrag['transaktionsId'], statusDok)
                else:
                    self.writeStatusApp(appName, "2", auftrag['transaktionsId'], '')



    def readRollenIds(self, listAuftraege):
        rollen = ['RZP_BERECHTIGTER', 'RZP_ZAHLUNGSEMPFAENGER', 'RZP_MITTEILUNGSEMPFAENGER', 'RZP_KONTOINHABER']
        connInfos = self.readConnectionInfos('PERF_IDENT')
        connClient = MongoClient(connInfos['connString'], uuidRepresentation="standard")
        opts = CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY)
        connDb = connClient[connInfos['db']]
        connColl = connDb[connInfos['collection']]

        for auftrag in listAuftraege:
            transaktionsIdConv = b64encode(uuid.UUID(auftrag['transaktionsId']).bytes).decode()
            rollenList = []
            anzRollenExistent = 0
            for rolle in rollen:
            #    listResult = connColl.find_one({"rollen": rolle,  "art": "OUTBOUND", "fachlicherStatus": "FREIGEGEBEN", "transaktionsId.binary.base64": transaktionsIdConv})
                listResult = connColl.find_one({
                                               "rollen": rolle,
                                               "art": "OUTBOUND",
                                               "fachlicherStatus": "FREIGEGEBEN",
                                               "transaktionsId": UUID(auftrag['transaktionsId'])})
                if listResult:
                    identitaetenId = listResult['identitaetenId']
                    personId = listResult['personId']
                    if rolle == "RZP_BERECHTIGTER":
                        postfix = "be"
                    elif rolle == "RZP_ZAHLUNGSEMPFAENGER":
                        postfix = "ze"
                    elif rolle == "RZP_MITTEILUNGSEMPFAENGER":
                        postfix = "me"
                    elif rolle == "RZP_KONTOINHABER":
                        postfix = "ki"
                    sqlUpdateRollen = "update transaktionIds set identitaetenId_" + str(postfix) + " = '" + str(identitaetenId) + "', personId_" + str(postfix) + " = '" + str(personId) + "' where transaktionsId = '" + str(auftrag['transaktionsId']) + "'"
                    self.db.execSql(sqlUpdateRollen, '')
                    rolleDict = {
                        'rolle': postfix,
                        'identitaetenId': str(identitaetenId),
                        'personId': str(personId)
                    }
                    rollenList.append(rolleDict)
                    anzRollenExistent = anzRollenExistent + 1

            if anzRollenExistent == 0:
                self.writeStatusApp("PERF_IDENT", "2", auftrag['transaktionsId'], '')
                self.writeStatusApp("PERF_PERS", "2", auftrag['transaktionsId'], '')


    def readDbInfos(self, appName):
        sql_valuelist = [appName + '%']
        sql = "select * from rzp_datenbanken where rzpDb like %s"
        dbInfos = self.db.execSelect(sql, sql_valuelist)
        return dbInfos[0]

    def readStatus(self, appName, transaktionsId, feldStatus, result):
        try:
            status = result[feldStatus]
            try:
                if appName in ("WCH", "KAUS"):
                    if isinstance(status, list):
                        anzStatus = len(status)
                        status = status[anzStatus-1]['status']
            except:
                print("Statusermittlung fehlgeschlagen", appName)
                status = "n.v."
        except:
            status = "Feld " + feldStatus + " nicht in Dokument gefunden"

        # Sonderfall Zielwelt WCH
        if appName == "WCH":
            try:
                zielwelt = result['routing']['zielwelt']
                sql_updateZielwelt = "update transaktionIds set zielwelt = '" + zielwelt + "' where transaktionsId = '" + transaktionsId + "'"
                self.db.execSql(sql_updateZielwelt, '')
            except:
                print("keine Routinginfos in WCH gefunden")

        return status

    def readStatusZielwelt(self, transaktionsId):
        sql_valueList = [transaktionsId]
        sql_readZielwelt = "select zielwelt from transaktionIds where transaktionsId = (%s)"
        zielwelt = self.db.execSelect(sql_readZielwelt, sql_valueList)
        return zielwelt[0]

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
 #       if transaktionsId == 'e1606b08-2343-432f-aa59-9b2413a439f2':
 #           print("Write Dokument", sql_writeDokument)
        self.db.execSql(sql_writeDokument, '')

    def writeStatusApp(self, appName, status, transaktionsId, statusDok):
        self.db = cls_dbAktionen()
        sql = "update transaktionIds set " + str(appName).lower() + " = '" + status + "', " + str(appName).lower() + "_status" + " = '" + statusDok + "' where transaktionsId = '" + transaktionsId + "'"
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

    def deleteCollection(self):
        self.db = cls_dbAktionen()
        sql = "select * from init_collections"
        collectionsToDelete = self.db.execSelect(sql, '')

        for collToDelete in collectionsToDelete:
            connInfos = self.readConnectionInfos(collToDelete['zielDb'])
            connClient = MongoClient(connInfos['connString'], uuidRepresentation="standard")
            opts = CodecOptions(uuid_representation=UuidRepresentation.PYTHON_LEGACY)
            connDb = connClient[connInfos['db']]
            connColl = connDb[collToDelete['collection']]
            d = connColl.delete_many({})



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
         #   self.writeKaus(transaktionsId, result)

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