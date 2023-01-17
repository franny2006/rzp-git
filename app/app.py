from flask import Flask, request, jsonify, render_template, redirect, flash, jsonify, abort
import requests
import json
import os

# from formulare import conForm, collForm
from pymongo import MongoClient
from classes.cls_parseZaToDb import cls_parseZa
from classes.cls_createDbSchema import cls_createSchema
from classes.cls_readAuftraege import cls_readAuftraege
from classes.cls_readMongo import cls_readMongo



app = Flask(__name__)
app.config["DEBUG"] = True
app.config['SECRET_KEY'] = 'you-will-never-guess'
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

readAuftraege = cls_readAuftraege()

try:
    with open('c:/temp/mongo-poc/config.json') as json_file:
        data = json.load(json_file)
except:
    with open('config/config.json') as json_file:
        data = json.load(json_file)
listConnections = []


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


### ZA-Datei importieren #############################################################
@app.route('/readAuftragsdatei')
def uploadFile():
    return render_template('readAuftragsdatei.html', title="Upload File")

@app.route('/readAuftragsdatei', methods=['POST'])
def uploadFile_submitted():
    strEncoding = "latin_1"
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        uploaded_file.save('uploads/' + uploaded_file.filename)
        importZa = cls_parseZa('uploads/' + uploaded_file.filename)
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

@app.route('/readRzp')
def readRzp():
    listAuftraege = []
    dictAuftrag = {}
    dictAuftrag['runId'] = request.args.get('runId')
    dictAuftrag['dsId'] = request.args.get('dsId')
    listAuftraege.append(dictAuftrag)
    try:
        getAll = request.args.get('getAll')
        if getAll == "1":
            alleAuftraege = readAuftraege.read_Auftraege_uebersicht()
            for auftragIds in alleAuftraege:
                dictAuftrag['runId'] = auftragIds['runId']
                dictAuftrag['dsId'] = auftragIds['dsId']
                listAuftraege.append({'runId': auftragIds['runId'], 'dsId': auftragIds['dsId']})
    except:
        getAll = False

    for auftrag in listAuftraege:
        dictAuftragUnique = readAuftraege.read_Auftrag_Unique(str(auftrag['runId']), str(auftrag['dsId']))

        # TransaktionsId ermitteln
        connMongo = cls_readMongo()
        transaktionsId = connMongo.readTransaktionsId(dictAuftragUnique[0]['panr'], dictAuftragUnique[0]['prnr'], dictAuftragUnique[0]['voat'], dictAuftragUnique[0]['lfdNr'])

        # Identitaeten- und PersonenIds für alle Rollen ermitteln
        identitaeten_personen_ids = connMongo.readRollenIds(transaktionsId['transaktionsId'])
     #   print(identitaeten_personen_ids)

        # Daten aus KAUS holen
        kaus = connMongo.readKaus(transaktionsId['transaktionsId'])

        #AAN
        aan = connMongo.readApplication(transaktionsId['transaktionsId'], "AAN", "transaktionsId", None, None, None, "sequenz")
        wch = connMongo.readApplication(transaktionsId['transaktionsId'], "WCH", "payload.transaktionsId", None, None, None, "_id")
        refue = connMongo.readApplication(transaktionsId['transaktionsId'], "REFUE", "transaktionsId", None, None, None, "_id")
        reza = connMongo.readApplication(transaktionsId['transaktionsId'], "REZA", "transaktionsId", None, None, None, "_id")

        # Identität und Person pro Rolle
        for rolle in identitaeten_personen_ids:
            if rolle['rolle'] == "ze":
                rolle_postfix = "ze"
                idIdentBez = "identitaetenId_ze"
                idPersonBez = "personId_ze"
            elif rolle['rolle'] == "be":
                rolle_postfix = "be"
                idIdentBez = "identitaetenId_be"
                idPersonBez = "personId_be"
            elif rolle['rolle'] == "me":
                rolle_postfix = "me"
                idIdentBez = "identitaetenId_me"
                idPersonBez = "personId_me"
            elif rolle['rolle'] == "ki":
                rolle_postfix = "ki"
                idIdentBez = "identitaetenId_ki"
                idPersonBez = "personId_ki"

            perf_ident = connMongo.readApplication(transaktionsId['transaktionsId'], "PERF_IDENT", "transaktionsId.binary.base64", rolle['rolle'], "identitaetenId.binary.base64", rolle['identitaetenId'], "_id")
            perf_pers = connMongo.readApplication(transaktionsId['transaktionsId'], "PERF_PERS", "transaktionsId.binary.base64", rolle['rolle'], "personId.binary.base64", rolle['personId'], "_id")


    return redirect('/showAuftraege')


@app.route('/showVergleichsreport')
def showVergleichsreport():
    from classes.cls_createFeatureFiles import cls_create_featureFiles
    # Feature-Files erzeugen und Verzeichnisse leeren
    createFeatureFiles = cls_create_featureFiles()

    os.system('behave features -f json.pretty -o export/reports/results.json')
    os.system('behave -f allure_behave.formatter:AllureFormatter -o export/allure-results/')
 #   os.system('behave -f allure_behave.formatter:AllureFormatter -o ./allure-results')
    report = open('export/reports/results.json')
    vergleichsReport = json.load(report)
#    vergleichsReport = [{}]
#    for report in vergleichsReport:
#        for scenario in report['elements']:
#            print("Scenario: ", scenario['keyword'], scenario['name'], scenario['status'])
#            for steps in scenario['steps']:
#                print("Steps: ", steps['keyword'], steps['name'], steps['result']['status'])
#                if steps['result']['status'] == "failed":
#                    print("Fehler:", steps['result']['error_message'])

#            print("*****************************************************************************************************************************************")

    return render_template('showVergleichsreport.html', title="Vergleichsreport", data=json.dumps(vergleichsReport, sort_keys = False, indent = 4, ensure_ascii=False))

@app.route('/viewConnections', methods=['GET', 'POST'])
def viewConnections():
    listConnections = []
    print("Conn in View:", listConnections)
    return render_template('viewConnections.html', title="Verbindungsübersicht", data=data['connection'])

@app.route('/checkConnection', methods=['GET', 'POST'])
def checkConnection():

    nameConnToCheck = request.args.get('nameConn')
    for connection in data['connection']:
        if connection['connectionName'] == nameConnToCheck:
            client = MongoClient(connection['connectionString'])
            response = client.server_info()
    return render_template('viewConnectionTestResult.html', title="Test der Verbindung", data=response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)