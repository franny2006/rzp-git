from behave import *
import json
from cls_db import cls_dbAktionen

db = cls_dbAktionen()
rzpDatenbanken = ['KUGA', 'AAN', 'WCH', 'PERF_IDENT', 'PERF_PERS', 'KAUS', 'RERE', 'REZA', 'REFUE']

@given('Es wurde eine Auftragsdatei eingespielt')
def step_init(context):
    pass
 #   print(transaktionsIds)

@given('Es wurde ein Auftrag mit PANR = {panr}, PRNR = {prnr}, VOAT = {voat}, lfdNr = {lfdNr}, TransaktionsId = {transaktionsId} eingespielt')
def step_givenAuftrag(context, panr, prnr, voat, lfdNr, transaktionsId):
    sql = "select count(transaktionsId) as anz from transaktionIds where transaktionsId = '" + str(transaktionsId) + "' and panr = '" + str(panr) + "' and prnr = '" + str(prnr) + "' and voat = '" + str(voat) + "' and lfdNr = '" + str(lfdNr) + "'"
 #   print(sql)
    anzTransaktionen = db.execSelect(sql, '')
    anzTransaktionen = anzTransaktionen[0]['anz']
    context.transaktionsId = transaktionsId
    context.panr = panr
    context.prnr = prnr
    context.voat = voat
    context.lfdNr = lfdNr

    assert anzTransaktionen == 1

@when('dieser Auftrag in der Datenbank {zielDb} gespeichert wurde')
def step_verarbeitungPruefen(context, zielDb):
    zielDb = zielDb.split(".")[0]
    sqlStatus = "select " + zielDb.lower() + " from transaktionIds where transaktionsId = '" + context.transaktionsId + "'"
    verarbeitungsStatus = db.execSelect(sqlStatus, '')
    dbFehlt = ""
    if verarbeitungsStatus[0][zielDb.lower()] != "1":
        dbFehlt = dbFehlt + str(zielDb) + " - "

    assert dbFehlt == "", f'Verarbeitung in Datenbank unvollständig: ' + dbFehlt


@then("enthaelt die Datenbank {zielDb} im Feld {zielFeld} den SOLL-Wert '{sollErgebnis}'. Urspruenglicher Auftragswert: '{inhaltAuftrag}' aus Feld '{ursprung}'")
def step_verifyFelder(context, zielDb, zielFeld, sollErgebnis, inhaltAuftrag, ursprung):
    # Dokumente ermitteln und in Dicts umwandeln

    listDokumente = []
  #  for app in rzpDatenbanken:
  #      sql = "select document from documents where transaktionsId = '" + context.transaktionsId + "' and herkunft = '" + app + "'"
    herkunft = zielDb.split('.')[0]
    sql = "select document from documents where transaktionsId = '" + context.transaktionsId + "' and herkunft = '" + herkunft + "'"
    resultDokument = db.execSelect(sql, '')
    dokument = {}
    dokument['herkunft'] = herkunft
    try:
        dokument['dokument'] = resultDokument[0]['document'].decode()
    except:
        dokument['dokument'] = "{}"
    listDokumente.append(dokument)
 #   print(listDokumente[0]['herkunft'])

    # Vergleich der Inhalte aus Auftragsdaten und Dokumenten-Dict
    zielDb = zielDb.split(".")[0]
    zielFeldDict = splitZielfeld(zielFeld)

    feldInhaltDokument = "nicht initialisiert"
    for dokument in listDokumente:
        if dokument['herkunft'] == zielDb:
            dictDokument = json.loads(dokument['dokument'])
            try:
                feldInhaltDokument = eval(zielFeldDict)
            except:
                feldInhaltDokument = "Feld nicht vorhanden"


    listBetragsfelder = ['betrag', 'euro', 'cent', 'rentenrechnungsdaten.zinsen', 'barzahlungskosten', 'rentenrechnungsdaten.abschlag', 'anzahl',
                         'entschaedigungsrente', 'Betrag', 'aufwendungen', 'beitrag', 'Beitrag', 'bemessungsgrundlage', 'zuschlag', 'zahlKinder', 'zahlWaisen']
    if any(ele in zielFeld for ele in listBetragsfelder):
        try:
            sollErgebnis = int(sollErgebnis)
        except:
            pass

    if str(sollErgebnis).strip() == "<leer>":
        assert feldInhaltDokument == "Feld nicht vorhanden" or feldInhaltDokument == "", f'SOLL: ' + str(sollErgebnis).strip() + ' - IST: ' + str(feldInhaltDokument)
    else:
        assert str(sollErgebnis).strip() == str(feldInhaltDokument), f'SOLL: ' + str(sollErgebnis).strip() + ' - IST: ' + str(feldInhaltDokument)



@then("enthaelt die Rolle {rolle} in der Datenbank {zielDb} zum Auftragswert {inhaltAuftrag} im Feld {zielFeld} den SOLL-Wert {sollErgebnis}")
def step_verifyFelderRollen(context, zielDb, rolle, zielFeld, inhaltAuftrag, sollErgebnis):
    # Dokumente ermitteln und in Dicts umwandeln
    listDokumente = []
    for app in rzpDatenbanken:
        sql = "select document from documents where transaktionsId = '" + context.transaktionsId + "' and herkunft = '" + app + "' and rolle = '" + rolle.lower() + "'"
  #      print(sql)
        resultDokument = db.execSelect(sql, '')
        dokument = {}
        dokument['herkunft'] = app
        try:
            dokument['dokument'] = resultDokument[0]['document'].decode()
        except:
            dokument['dokument'] = "{}"
        listDokumente.append(dokument)
  #  print("Dokumente: ", listDokumente)

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

    listBetragsfelder = ['betrag', 'euro']
    if any(ele in zielFeld for ele in listBetragsfelder):
        sollErgebnis = sollErgebnis.lstrip("0")
    if sollErgebnis.strip() == "<leer>":
        assert feldInhaltDokument == "Feld nicht vorhanden" or feldInhaltDokument == "", f'SOLL: ' + str(sollErgebnis.strip()) + ' - IST: ' + str(feldInhaltDokument)
    else:
        assert str(sollErgebnis.strip()) == str(feldInhaltDokument), f'SOLL: ' + str(sollErgebnis.strip()) + ' - IST: ' + str(feldInhaltDokument)




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
        feldInhaltAuftrag = "nicht initialisiert"

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
            feldInhaltDokument = "Zielfeld konnte nicht gefunden werden"
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

def konvertierungsregel_anwenden(regel, inhaltAuftrag):
    from datetime import datetime
    if regel == "datum":
        inhaltAuftrag = datetime.strptime(inhaltAuftrag, '%Y%m%d').date()
    elif regel.lower() == "datum_yyyymm":
        dataTemp = datetime.strptime(inhaltAuftrag, '%Y%m').date()
        inhaltAuftrag = dataTemp.strftime('%Y-%m')
    elif regel[:3].lower() == 'ps_':
        feldAuftrag = regel[3:]
        sql = "select distinct keyRzp from schluessel where feldAuftrag = '" + str(feldAuftrag) + "' and keyAuftrag = '" + str(inhaltAuftrag) + "'"
        valueZiel = db.execSelect(sql, '')
        try:
            inhaltAuftrag = valueZiel[0]['keyRzp'].upper()
        except:
            inhaltAuftrag = "kein Mapping vorhanden für Wert " + str(inhaltAuftrag) + " in Feld " + str(feldAuftrag)
        print(inhaltAuftrag)

    return(str(inhaltAuftrag))

