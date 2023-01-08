from behave import *
import json
from cls_db import cls_dbAktionen

db = cls_dbAktionen()
rzpDatenbanken = ['KUGA', 'AAN', 'WCH', 'KAUS']

@given('Es wurde eine Auftragsdatei eingespielt')
def step_init(context):
    pass
 #   print(transaktionsIds)

@given('Es wurde ein Auftrag mit PANR = {panr}, PRNR = {prnr}, VOAT = {voat}, lfdNr = {lfdNr}, TransaktionsId = {transaktionsId} eingespielt')
def step_givenAuftrag(context, panr, prnr, voat, lfdNr, transaktionsId):
    sql = "select count(transaktionsId) as anz from transaktionIds where transaktionsId = '" + str(transaktionsId) + "' and panr = '" + str(panr) + "' and prnr = '" + str(prnr) + "' and voat = '" + str(voat) + "' and lfdNr = '" + str(lfdNr) + "'"
    print(sql)
    anzTransaktionen = db.execSelect(sql, '')
    anzTransaktionen = anzTransaktionen[0]['anz']
    context.transaktionsId = transaktionsId
    context.panr = panr
    context.prnr = prnr
    context.voat = voat
    context.lfdNr = lfdNr

    assert anzTransaktionen == 1

@when('dieser Auftrag vollstaendig verarbeitet wurde')
def step_verarbeitungPruefen(context):
    sqlStatus = "select * from transaktionIds where transaktionsId = '" + context.transaktionsId + "'"
    verarbeitungsStatus = db.execSelect(sqlStatus, '')
    dbFehlt = ""
    for rzpDb in rzpDatenbanken:
        if verarbeitungsStatus[0][rzpDb.lower()] != "1":
            dbFehlt = dbFehlt + str(rzpDb) + " - "

    assert dbFehlt == "", f'Verarbeitung in Datenbanken unvollständig: ' + dbFehlt

@then('enthaelt in der Datenbank {zielDb} das Feld {zielFeld} den gleichen Inhalt wie {feldAuftrag}')
def step_verifyFelder(context, zielDb, zielFeld, feldAuftrag):
    # Auftragdaten ermitteln
    # DS-ID ermitteln
    auftragBereiche = feldAuftrag.split(".")
    tabelleSatzart = auftragBereiche[0]
    feldNameSatzart = auftragBereiche[1]
    sqlIdentifier = "select runId, dsId from sa_ft where panr = '" + context.panr + "' and prnr = '" + context.prnr + "' and voat = '" + context.voat + "' and laufendeNummerZl = '" + context.lfdNr + "'"
    auftragsIdentifier = db.execSelect(sqlIdentifier, '')

    sqlFeldinhalt = "select " + feldNameSatzart + " from " + tabelleSatzart.lower() + " where runId = '" + str(auftragsIdentifier[0]['runId']) + "' and dsId = '" + str(auftragsIdentifier[0]['dsId']) + "'"
    auftragFeldinhalt = db.execSelect(sqlFeldinhalt, '')
    dictAuftrag = {
        'auftragRunId': auftragsIdentifier[0]['runId'],
        'auftragDsId': auftragsIdentifier[0]['dsId'],
        'auftragFeldname': feldNameSatzart,
        'auftragFeldinhalt': auftragFeldinhalt[0][feldNameSatzart]
    }
    print("Auftragsdaten:", dictAuftrag)

    # Dokumente ermitteln und in Dicts umwandeln
    listDokumente = []
    for app in rzpDatenbanken:
        sql = "select document from documents where transaktionsId = '" + context.transaktionsId + "' and herkunft = '" + app + "'"
        resultDokument = db.execSelect(sql, '')
        dokument = {}
        dokument['herkunft'] = app
        try:
            dokument['dokument'] = resultDokument[0]['document'].decode()
        except:
            dokument['dokument'] = "{}"
        listDokumente.append(dokument)
    print(listDokumente[0]['herkunft'])

    # Vergleich der Inhalte aus Auftragsdaten und Dokumenten-Dict
    zielDb = zielDb.split(".")[0]
    zielFeldDict = splitZielfeld(zielFeld)

    for dokument in listDokumente:
        if dokument['herkunft'] == zielDb:
            dictDokument = json.loads(dokument['dokument'])
     #       print(dictDokument)
            try:
                feldInhaltDokument = eval(zielFeldDict)
            except:
                feldInhaltDokument = "Feld nicht vorhanden"

    assert dictAuftrag['auftragFeldinhalt'].strip() == feldInhaltDokument, f'SOLL: ' + str(dictAuftrag['auftragFeldinhalt']) + ' - IST: ' + str(feldInhaltDokument)






@when('das Feld {feldAuftrag} gefüllt ist')
def step_ursprungswertErmitteln(context, feldAuftrag):
    db = cls_dbAktionen()
    # zerlegen des Auftragsfeldes
    auftragBereiche = feldAuftrag.split(".")
 #   satzart = auftragBereiche[0].split("_")[1]
    tabelle = auftragBereiche[0]
    feldName = auftragBereiche[1]
    auftrag = {}
    for transaktion in context.listTransaktionen:
        context.listTransaktionen
        # DS-ID ermitteln
        sqlIdentifier = "select runId, dsId from sa_ft where panr = '" + transaktion['panr'] + "' and prnr = '" + transaktion['prnr'] + "' and voat = '" + transaktion['voat'] + "' and laufendeNummerZl = '" + transaktion['lfdNr'] + "'"
        auftragsIdentifier = db.execSelect(sqlIdentifier, '')

        sqlFeldinhalt = "select " + feldName + " from " + tabelle.lower() + " where runId = '" + str(auftragsIdentifier[0]['runId']) + "' and dsId = '" + str(auftragsIdentifier[0]['dsId']) + "'"
        auftragFeldinhalt = db.execSelect(sqlFeldinhalt, '')
        dictAuftrag = {
            'auftragRunId': auftragsIdentifier[0]['runId'],
            'auftragDsId': auftragsIdentifier[0]['dsId'],
            'auftragFeldname': feldName,
            'auftragFeldinhalt': auftragFeldinhalt[0][feldName]
        }
        transaktion['auftragsdaten'].append(dictAuftrag)

  #  print("TransaktionsId: ", transaktion['transaktionsId'], transaktion['prnr'], auftrag)
 #   print(context.listTransaktionen)
    pass



@then('___enthaelt in der Datenbank {zielDb} das Feld {zielFeld} den gleichen Inhalt wie {feldAuftrag} _alt')
def step_verifyFeld_alt(context, zielDb, zielFeld, feldAuftrag):
    anzFehler = 0
    feldAuftrag = feldAuftrag.split(".")[1]
    zielDb = zielDb.split(".")[0]
    zielFeld = splitZielfeld(zielFeld)

   # print("zu prüfen: ", feldAuftrag, zielFeld, zielDb)
    for transaktion in context.listTransaktionen:
   #     print("************************************************* Start **************************************")
   #     print(transaktion['auftragsdaten'])
        dokumente = {}
        for dictDokument in transaktion['dokumente']:
            dokumente[dictDokument['herkunft']] = json.loads(dictDokument['dokument'])

        # Wert zur ZielDB passende Dokument suchen
        for herkunft, dokument in dokumente.items():
       #     print("******* Start Dokument *********", zielDb, herkunft)
            if herkunft == zielDb:
  #              print(dokument)

                try:
                    feldInhaltDokument = eval(zielFeld)
                except:
                    feldInhaltDokument = "Feld nicht vorhanden"

       #     print("******* Ende Dokument *********")
       #     pass

        for feldDatenAuftrag in transaktion['auftragsdaten']:
    #        print("zu prüfen 2: ", feldAuftrag, feldDatenAuftrag)
            for feld in feldDatenAuftrag.values():
                if feldAuftrag == feld:
                    feldInhaltAuftrag = feldDatenAuftrag['auftragFeldinhalt']
                #    print("Feld in Auftrag gefunden:", zielDb, zielFeld, feldInhaltAuftrag)


        if feldInhaltAuftrag.strip() != feldInhaltDokument:
            print(feldAuftrag + ": " + feldInhaltAuftrag)
            print(zielFeld + ": " + feldInhaltDokument)
            anzFehler = anzFehler + 1

  #      assert feldInhaltAuftrag.strip() == feldInhaltDokument, f'erwartet: ' + feldInhaltAuftrag.strip() + " --- IST: " + feldInhaltDokument + " bei TransaktionsId " + transaktion['transaktionsId']
        assert anzFehler == 0, f'Anzahl Fehler: ' + str(anzFehler)
 #   print("************************************************* Ende **************************************")




def splitZielfeld(zielFeld):
    # Zielfeld ermitteln
    listStrukturZielfeld = zielFeld.split(".")
    k = 0
    for hierarchieZielfeld in listStrukturZielfeld:
        if k == 0:
            zielFeld = "dictDokument['" + hierarchieZielfeld + "']"
        else:
            zielFeld = zielFeld + "['" + hierarchieZielfeld + "']"
        k = k + 1
    return zielFeld
