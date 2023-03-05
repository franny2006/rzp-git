from flask import Flask, request, jsonify, render_template, redirect, flash, jsonify, abort
import requests
import json
import os
from configparser import ConfigParser
import time

# from formulare import conForm, collForm
from pymongo import MongoClient
from classes.cls_parseZaToDb import cls_parseZa
from classes.cls_createDbSchema import cls_createSchema
from classes.cls_readAuftraege import cls_readAuftraege
from classes.cls_readMappings import cls_readMappings
from classes.cls_readCollectionsToInitialize import cls_readCollectionsToInit
from classes.cls_readMongo import cls_readMongo



app = Flask(__name__)
app.config["DEBUG"] = True
app.config['SECRET_KEY'] = 'you-will-never-guess'
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

readAuftraege = cls_readAuftraege()




@app.context_processor
def utilities():
    def status(transaktionsId, zielweltSoll: str, zielweltIst: str):
        if transaktionsId:
            transaktionsStati = readAuftraege.read_transaktionsStatus(transaktionsId)
            print("Stati:", transaktionsStati)
            fehler = 0
            if zielweltIst != None:
                if zielweltSoll != None:
                    if zielweltSoll[:3].lower() == "rzp":
                        if zielweltIst.lower() != "neu":
                            fehler = fehler + 1
                    elif zielweltSoll[:4].lower() == "reds":
                        if zielweltIst.lower() != "alt":
                            fehler = fehler + 1
            for collection, collStatus in transaktionsStati.items():
                if collection.lower() not in ('zielwelt'):
                    try:
                        if transaktionsStati['zielwelt'].lower() == 'neu':
                            if collStatus == None or int(collStatus) == 2:
                                fehler = fehler + 1
                        elif transaktionsStati['zielwelt'].lower() == 'alt':
                            if collection in ('KUGA', 'AAN', 'AUA', 'WCH'):
                                if collStatus == None or int(collStatus) == 2:
                                    fehler = fehler + 1
                    except:
                        fehler = fehler +1


            if fehler > 0:
                status = "nok"
            elif fehler == 0:
                status = 'ok'
            else:
                status = "n.d."
        else:
            status = "leer"
        return status
    return dict(status=status)

@app.route('/', methods=['GET'])
def home():
    return render_template('base.html')

### DB zurücksetzen##### #############################################################
@app.route('/reset')
def resetGui():
    return render_template('resetTables.html', title="Datenbank zurücksetzen")

@app.route('/reset', methods=['POST'])
def resetDb():
    cls_createSchema()
    return redirect('/')

### Mappingregeln importieren ########################################################
@app.route('/readMappings')
def uploadMapping():
    return render_template('readMappings.html', title="Upload File")

@app.route('/readMappings', methods=['POST'])
def uploadMapping_submitted():
    strEncoding = "latin_1"
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        uploaded_file.save('uploads/' + uploaded_file.filename)
        importMappings = cls_readMappings('uploads/' + uploaded_file.filename)
    return redirect('/showAuftraege')

### Configdatein der zu initialisierende Collections importieren ########################################################
@app.route('/readCollectionsToInitialize')
def uploadCollectionsToInit():
    return render_template('readCollectionsToInitialize.html', title="Upload File")

@app.route('/readCollectionsToInitialize', methods=['POST'])
def uploadCollectionsToInit_submitted():
    strEncoding = "latin_1"
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        uploaded_file.save('uploads/' + uploaded_file.filename)
        importFile = cls_readCollectionsToInit('uploads/' + uploaded_file.filename)
    return redirect('/showAuftraege')


### ZA-Datei importieren #############################################################
@app.route('/readAuftragsdatei')
def uploadFile():
    return render_template('readAuftragsdatei.html', title="Upload File")

@app.route('/readAuftragsdatei', methods=['POST'])
def uploadFile_submitted():
    strEncoding = "latin_1"
    uploaded_file = request.files['file']
    sendungsnummer = request.form['sendungsnummer']
    if uploaded_file.filename != '':
        uploaded_file.save('uploads/' + uploaded_file.filename)
        importZa = cls_parseZa('uploads/' + uploaded_file.filename, sendungsnummer)
    return redirect('/showAuftraege')


### Aufträge anzeigen ################################################################
@app.route('/showAuftraege')
def showOrders():
    dictAuftraege = readAuftraege.read_Auftraege_uebersicht()

    return render_template('showAuftraege.html', title="Auftragsübersicht", data=dictAuftraege)

@app.route('/readDocument')
def readDocument():
    herkunft = request.args.get('herkunft')
    transaktionsId = request.args.get('transaktionsId')
    jsonDocument = readAuftraege.read_document(herkunft, transaktionsId)
    json_object = json.loads(jsonDocument[0]['document'])

 #   json_object = json.dumps(json_object, indent=4)

    return render_template('document.html', title=herkunft, data=json.dumps(json_object, sort_keys = False, indent = 4, ensure_ascii=False))

@app.route('/showAuftragDetails')
def showAuftragDetails():
    runId = request.args.get('runId')
    dsId = request.args.get('dsId')
    dictAuftragZa = readAuftraege.read_Auftrag(runId, dsId)
    dictStatusApps = readAuftraege.read_AuftragStatusApps(runId, dsId)
    dictAuftrag = {
        "auftragZa": dictAuftragZa,
        "statusApps": dictStatusApps[0]
    }
    return render_template('showAuftragDetails.html', title="Auftrag", data=dictAuftrag)

@app.route('/readRzp', methods = ['POST', 'GET'])
def readRzp():
    zeitanfang = time.time()
    listAuftraege = []
    dictAuftrag = {}
    dictAuftrag['runId'] = request.form.get('runId', False)
    dictAuftrag['dsId'] = request.form.get('dsId', False)
    dictAuftrag['sendungsnummer'] = request.form.get('sendungsnummer', False)
    dictAuftrag['datei'] = request.form.get('datei', False)
    listAuftraege.append(dictAuftrag)
    try:
        getAll = request.args.get('getAll')
        if getAll == "1":
            alleAuftraege = readAuftraege.read_Auftraege_uebersicht()
            for auftragIds in alleAuftraege:
                dictAuftrag['runId'] = auftragIds['runId']
                dictAuftrag['dsId'] = auftragIds['dsId']
                dictAuftrag['sendungsnummer'] = auftragIds['za_sendungsnummer']
                dictAuftrag['datei'] = auftragIds['za_datei']
                listAuftraege.append({'runId': auftragIds['runId'], 'dsId': auftragIds['dsId'], 'sendungsnummer': auftragIds['za_sendungsnummer'], 'datei': auftragIds['za_datei']})
    except:
        getAll = False

    messpunkt1 = time.time()
    print("Messpunkt 1 Aufträge gesammelt:", messpunkt1-zeitanfang)

    connMongo = cls_readMongo()
    # TransaktionsIds ermitteln und KUGA-Dokumente speichern
    transaktionsIds = connMongo.readTransaktionsIds(listAuftraege)

    messpunkt2 = time.time()
    print("Messpunkt 2 TransaktionsIds ermittelt:", messpunkt2 - messpunkt1)

    # Aufträge um gefundene TransaktionsIds anreichern
    for auftrag in listAuftraege:
        auftragsInfos = readAuftraege.read_AuftragStatusApps(auftrag['runId'], auftrag['dsId'])
        auftrag['transaktionsId'] = auftragsInfos[0]['transaktionsId']
    messpunkt3 = time.time()
    print("Messpunkt 3 Auftragsliste erweitert:", messpunkt3 - messpunkt2)


    # AAN-Dokumente speichern
    aan = connMongo.readApplication("AAN", listAuftraege, None, None)
    wch = connMongo.readApplication("WCH", listAuftraege, "PRIMAER", None)

    # Aufträge um Zielwelt anreichern
    listAuftraegeRzp = []
    for auftrag in listAuftraege:
        auftragsInfos = readAuftraege.read_AuftragStatusApps(auftrag['runId'], auftrag['dsId'])
        auftrag['zielwelt'] = auftragsInfos[0]['zielwelt']
        if auftrag['zielwelt']:
            if auftrag['zielwelt'].lower() == 'neu':
                listAuftraegeRzp.append(auftrag)
    messpunkt4 = time.time()
    print("Messpunkt 4 Zielwelt angereichert:", messpunkt4 - messpunkt3)

    # Aufträge um Identitäten und Personen anreichern
    identitaeten_personen_ids = connMongo.readRollenIds(listAuftraegeRzp)
    for auftrag in listAuftraegeRzp:
        auftragsInfos = readAuftraege.read_AuftragStatusApps(auftrag['runId'], auftrag['dsId'])
        auftrag['identitaetenId_ze'] = auftragsInfos[0]['identitaetenId_ze']
        auftrag['personId_ze'] = auftragsInfos[0]['personId_ze']
        auftrag['identitaetenId_be'] = auftragsInfos[0]['identitaetenId_be']
        auftrag['personId_be'] = auftragsInfos[0]['personId_be']
        auftrag['identitaetenId_me'] = auftragsInfos[0]['identitaetenId_me']
        auftrag['personId_me'] = auftragsInfos[0]['personId_me']
        auftrag['identitaetenId_ki'] = auftragsInfos[0]['identitaetenId_ki']
        auftrag['personId_ki'] = auftragsInfos[0]['personId_ki']

    messpunkt5 = time.time()
    print("Messpunkt 5 Ids für Person und Identität angereichert:", messpunkt5 - messpunkt4)

    # Dokumente aus Identitäten und Person pro Rolle abholen
    rollen = ['ze', 'be', 'me', 'ki']
    for rolle in rollen:
        perf_ident = connMongo.readApplication("PERF_IDENT", listAuftraegeRzp, None, rolle)
        perf_pers = connMongo.readApplication("PERF_PERS", listAuftraegeRzp, None, rolle)
    messpunkt6 = time.time()
    print("Messpunkt 6 Dokumente für alle Rollen abgeholt:", messpunkt6 - messpunkt5)

    rere = connMongo.readApplication("RERE", listAuftraegeRzp, None, None)
    refue = connMongo.readApplication("REFUE", listAuftraegeRzp, None, None)
    reza = connMongo.readApplication("REZA", listAuftraegeRzp, None, None)
    kaus = connMongo.readApplication("KAUS", listAuftraegeRzp, None, None)



    messpunkt7 = time.time()
    print("Messpunkt 7 Ende:", messpunkt7 - messpunkt6)
    print("Gesamte Laufzeit:", messpunkt7 - zeitanfang)

    return redirect('/showAuftraege')



@app.route('/executeTests')
def executeTests():
    return render_template('executeTests.html', title="Tests ausführen")


@app.route('/executeTests', methods=['POST'])
def executeTests_submitted():
    from classes.cls_createFeatureFiles import cls_create_featureFiles
    from classes.cls_createExecutionReport import cls_createExecutionReport
    # Feature-Files erzeugen und Verzeichnisse leeren
    createFeatureFiles = cls_create_featureFiles()

    reportType = request.form.get('reportType', False)
    print("Report", reportType)
    if reportType == "allure":
        os.system('behave -f allure_behave.formatter:AllureFormatter -o export/allure-results/')
    else:
        os.system('behave features -f json.pretty -o export/reports/results.json')
        report = cls_createExecutionReport()
    return redirect('/showAuftraege')


@app.route('/showVergleichsreport', methods=['POST'])
def showVergleichsreport():
    from classes.cls_createFeatureFiles import cls_create_featureFiles
    # Feature-Files erzeugen und Verzeichnisse leeren
    createFeatureFiles = cls_create_featureFiles()

    os.system('behave features -f json.pretty -o export/reports/results.json')
    os.system('behave -f allure_behave.formatter:AllureFormatter -o export/allure-results/')


 #   os.system('behave -f allure_behave.formatter:AllureFormatter -o ./allure-results')
 #   report = open('export/reports/results.json')
 #   vergleichsReport = json.load(report)


  #  return render_template('showVergleichsreport.html', title="Vergleichsreport", data=json.dumps(vergleichsReport, sort_keys = False, indent = 4, ensure_ascii=False))
    return redirect('/showAuftraege')






config = ConfigParser()
config.read('config/config.ini')
if config.get('Installation', 'zielumgebung').lower() == "local":
    ziel = 'mongo-db-config local'
else:
    ziel = 'mongo-db-config docker'

with open(config.get(ziel, 'configfile')) as json_file:
    data = json.load(json_file)


@app.route('/viewConnections', methods=['GET', 'POST'])
def viewConnections():
    listConnections = []
    return render_template('viewConnections.html', title="Verbindungsübersicht", data=data['connection'])

@app.route('/checkConnection', methods=['GET', 'POST'])
def checkConnection():

    nameConnToCheck = request.args.get('nameConn')
    for connection in data['connection']:
        if connection['connectionName'] == nameConnToCheck:
            client = MongoClient(connection['connectionString'])
            response = client.server_info()
    return render_template('viewConnectionTestResult.html', title="Test der Verbindung", data=response)

@app.route('/initCollections', methods=['GET', 'POST'])
def initCollections():
    connMongo = cls_readMongo()
    connMongo.deleteCollection()

    return redirect('/showAuftraege')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)