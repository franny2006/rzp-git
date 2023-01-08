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
    return redirect('/')


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
    runId = request.args.get('runId')
    dsId = request.args.get('dsId')
    dictAuftragUnique = readAuftraege.read_Auftrag_Unique(runId, dsId)
    print(dictAuftragUnique)
    # TransaktionsId ermitteln
    connMongo = cls_readMongo()
    transaktionsId = connMongo.readTransaktionsId(dictAuftragUnique[0]['panr'], dictAuftragUnique[0]['prnr'], dictAuftragUnique[0]['voat'], dictAuftragUnique[0]['lfdNr'])
    print(transaktionsId['transaktionsId'])

    # Daten aus KAUS holen
    kaus = connMongo.readKaus(transaktionsId['transaktionsId'])

    #AAN
    aan = connMongo.readApplication(transaktionsId['transaktionsId'], "AAN", "transaktionsId", "sequenz")
    wch = connMongo.readApplication(transaktionsId['transaktionsId'], "WCH", "payload.transaktionsId", "statusHistorie")

    return redirect('/showAuftraege')


@app.route('/showVergleichsreport')
def showVergleichsreport():
    os.system('behave features -f json.pretty -o features/reports/results.json')
    report = open('features/reports/results.json')
    vergleichsReport = json.load(report)
#    for report in vergleichsReport:
#        for scenario in report['elements']:
#            print("Scenario: ", scenario['keyword'], scenario['name'], scenario['status'])
#            for steps in scenario['steps']:
#                print("Steps: ", steps['keyword'], steps['name'], steps['result']['status'])
#                if steps['result']['status'] == "failed":
#                    print("Fehler:", steps['result']['error_message'])

#            print("*****************************************************************************************************************************************")

    return render_template('showVergleichsreport.html', title="Vergleichsreport", data=json.dumps(vergleichsReport, sort_keys = False, indent = 4, ensure_ascii=False))



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)