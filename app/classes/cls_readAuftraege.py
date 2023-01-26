import collections
import json

from .cls_db import cls_dbAktionen

class cls_readAuftraege():
    def __init__(self):
        herkunft = "testdaten_leer"  # leer, testdatenEkl, testdatenEkl_Prod, testdaten_GIT, testdaten_GIT2
        self.db = cls_dbAktionen(herkunft)

    def read_Auftraege_uebersicht(self):
        sql = "select za.*, rzp.*, ' ' as rolleZe, ' ' as rolleBe, ' ' as rolleMe, sa_11.zunameZUNAME as jiraId, sa_11.vornameVORNAME as ziel, sa_14.adresszusatzZahlungsempfaengerADRZUS as titel " \
              "from (select a.runId, a.dsId, laufendeNummerZL, panr as za_panr, prnr as za_prnr, voat as za_voat, datei from sa_ft a, runs b where a.runId = b.Id) za " \
              "left join transaktionIds rzp " \
              " on za.za_panr = rzp.panr and za.za_prnr=rzp.prnr and za.za_voat=rzp.voat and za.laufendeNummerZl = rzp.lfdNr " \
              "left join sa_14 " \
              " on sa_14.runId = za.runId and sa_14.dsId = za.dsId " \
              "left join sa_11 " \
              " on sa_11.runId = za.runId and sa_11.dsId = za.dsId "
        auftraege = self.db.execSelect(sql, '')
        for auftrag in auftraege:
            if auftrag['za_voat'] in ('21'):
                rollen = self.read_Rollen_Auftrag(auftrag['runId'], auftrag['dsId'])

                auftrag['rolleZe'] = rollen['rolleZe']
                auftrag['rolleBe'] = rollen['rolleBe']
                auftrag['rolleMe'] = rollen['rolleMe']
            else:
                auftrag['rolleZe'] = ""
                auftrag['rolleBe'] = ""
                auftrag['rolleMe'] = ""
                #    print("Auftraege: ", auftraege)
        return auftraege


    def read_Auftrag(self, runId, dsId):
        sql = "select za.*, rzp.* from (select * from V_DS10_komplett where runId = " + runId + " and dsId = " + dsId + ") za " \
              "left join transaktionIds rzp on " \
              "za.panr = rzp.panr and za.prnr=rzp.prnr and za.voat=rzp.voat and za.laufendeNummerZl = rzp.lfdNr"
        auftrag = self.db.execSelect(sql, '')
        auftrag = self.structureIntoSa(auftrag)
        return auftrag

    def read_Rollen_Auftrag(self, runId, dsId):
        sql = "select ze.anredeschluesselANREDSC, ze.vornameVORNAME, ze.zunameZUNAME, " \
              "be.anredeschluesselBerechtigterANREDSCBC, be.vornameBerechtigterVORNAMEBC, be.zunameBerechtigterZUNAMEBC, " \
              "me.anredeschluesselMitteilungsempfaengerANREDSCMT, me.vornameMitteilungsempfaengerVORNAMEMT, me.zunameMitteilungsempfaengerZUNAMEMT, " \
              "rechtsstellungZahlungsempfaengerBerechtigterRCZE, rechtsstellungMitteilungsempfaengerBerechtigterRCMT " \
              "from sa_11 ze " \
              "left join sa_19 be " \
              "on ze.runId = be.runId and ze.dsId = be.dsId " \
              "left join sa_m1 me " \
              "on ze.runId = me.runId and ze.dsId = me.dsId " \
              "left join sa_95 " \
              "on ze.runId = sa_95.runId and ze.dsId = sa_95.dsId " \
              "where ze.runId = '" + str(runId) + "' and ze.dsId = '" + str(dsId) + "'"

        rollen = self.db.execSelect(sql, '')
        ze = str(rollen[0]['anredeschluesselANREDSC']) + str(rollen[0]['vornameVORNAME']) + str(rollen[0]['zunameZUNAME'])
        be = str(rollen[0]['anredeschluesselBerechtigterANREDSCBC']) + str(rollen[0]['vornameBerechtigterVORNAMEBC']) + str(rollen[0]['zunameBerechtigterZUNAMEBC'])
        me = str(rollen[0]['anredeschluesselMitteilungsempfaengerANREDSCMT']) + str(rollen[0]['vornameMitteilungsempfaengerVORNAMEMT']) + str(rollen[0]['zunameMitteilungsempfaengerZUNAMEMT'])

        rollen[0]['rolleZe'] = ' '
        rollen[0]['rolleBe'] = ' '
        rollen[0]['rolleMe'] = ' '
        if ze != be and be != 'NoneNoneNone':
            rollen[0]['rolleBe'] = "abw. BE"
        if ze != me and me != 'NoneNoneNone':
            rollen[0]['rolleMe'] = "abw. ME"
        return rollen[0]

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