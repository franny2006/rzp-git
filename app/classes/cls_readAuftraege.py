import collections
import json

from .cls_db import cls_dbAktionen

class cls_readAuftraege():
    def __init__(self):
        herkunft = "testdaten_leer"  # leer, testdatenEkl, testdatenEkl_Prod, testdaten_GIT, testdaten_GIT2
        self.db = cls_dbAktionen(herkunft)

    def read_Auftraege_uebersicht(self):
        sql = "select za.*, rzp.* " \
              "from (select a.runId, a.dsId, laufendeNummerZL, panr, prnr, voat, datei from sa_ft a, runs b where a.runId = b.Id) za " \
              "left join transaktionIds rzp on " \
              "za.panr = rzp.panr and za.prnr=rzp.prnr and za.voat=rzp.voat and za.laufendeNummerZl = rzp.lfdNr"
        auftraege = self.db.execSelect(sql, '')
        return auftraege


    def read_Auftrag(self, runId, dsId):
        sql = "select za.*, rzp.* from (select * from V_DS10_komplett where runId = " + runId + " and dsId = " + dsId + ") za " \
              "left join transaktionIds rzp on " \
              "za.panr = rzp.panr and za.prnr=rzp.prnr and za.voat=rzp.voat and za.laufendeNummerZl = rzp.lfdNr"
        auftrag = self.db.execSelect(sql, '')
        auftrag = self.structureIntoSa(auftrag)
        return auftrag

    def read_AuftragStatusApps(self, runId, dsId):
        sql = "select za.*, rzp.* from (select panr, prnr, voat, laufendeNummerZl from V_DS10_komplett where runId = " + runId + " and dsId = " + dsId + ") za " \
              "left join transaktionIds rzp on " \
              "za.panr = rzp.panr and za.prnr=rzp.prnr and za.voat=rzp.voat and za.laufendeNummerZl = rzp.lfdNr"
        status = self.db.execSelect(sql, '')
        return status

    def read_Auftrag_Unique(self, runId, dsId):
        sql = "select panr, prnr, voat, laufendeNummerZL as lfdNr from V_DS10_komplett where runId = " + runId + " and dsId = " + dsId
        auftragUnique = self.db.execSelect(sql, '')
        return auftragUnique

    def read_document(self, herkunft, transaktionsId):
        sql = "select * from documents where herkunft = '" + herkunft + "' and transaktionsId = '" + transaktionsId + "'"
        document = self.db.execSelect(sql, '')
        return document

    def structureIntoSa(self, auftrag):
        with open('config/saCluster.json') as json_file:
            dictSaStruktur = json.load(json_file)

        dictAuftragStruktur = {}
   #     print(auftrag)
        listeEintraege = []
        for feldname, feldinhalt in auftrag[0].items():                                 # Durch Auftrag iterieren
            listSaAuftrag = feldname.split("_")                                # Feldnamen zerlegen (um Satzart aus jedem Feld abzuleiten)
            if listSaAuftrag[0].lower() == "sa":                                # Einleitende Felder ignorieren (haben keine SA-Bezeichnung)
                saBez = listSaAuftrag[1]                                # SA aus jedem Feld ableiten
                for bereich in dictSaStruktur:
                    if bereich['name'] not in dictAuftragStruktur.keys():
                        dictAuftragStruktur[bereich['name']] = {}
                    if saBez.upper() in bereich['satzarten'].keys():
                        if saBez.upper() not in dictAuftragStruktur[bereich['name']].keys():
                            dictAuftragStruktur[bereich['name']][saBez.upper()] = {}
                        # Feldname um Prefix kürzen
                        feldname = listSaAuftrag[2]
                        # Ignorieren von DS-ID und RunId
                        if feldname not in ('dsId', 'runId') and 'satzartbezeichnung' not in feldname:
                            dictAuftragStruktur[bereich['name']][saBez.upper()][feldname] = feldinhalt
   #     print(dictAuftragStruktur)
        return(dictAuftragStruktur)







        return auftrag