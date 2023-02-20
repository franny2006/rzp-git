from behave import fixture, use_fixture
from steps.cls_db import cls_dbAktionen


@fixture
def readTransaktionIds(context):
    print("Jetzt TransaktionsIds lesen")
    db = cls_dbAktionen()
    # Alle Aufträge lesen
    sql = "select panr, prnr, voat, lfdNr, transaktionsId from transaktionIds where anzFehler = 0"
    datensaetze = db.execSelect(sql, '')
    for datensatz in datensaetze:
        datensatz['auftragsdaten'] = []
    context.listTransaktionen = datensaetze



    # Dokumente lesen
    herkunft = ['KUGA', 'AAN', 'WCH', 'KAUS']
    for transaktion in context.listTransaktionen:
        transaktion['dokumente'] = []
        i=0
        for app in herkunft:
            sql = "select document from documents where transaktionsId = '" + transaktion['transaktionsId'] + "' and herkunft = '" + app + "'"
            resultDokument = db.execSelect(sql, '')
            dokument = {}
            dokument['herkunft'] = app
            try:
                dokument['dokument'] = resultDokument[0]['document'].decode()
            except:
                dokument['dokument'] = "{}"
            transaktion['dokumente'].append(dokument)
            i = i + 1

    print("Context fix: ", context.listTransaktionen)


# before all
def before_all(context):
    print('Before all executed')
  #  use_fixture(readTransaktionIds, context)

# before every feature
def before_feature(feature, context):
    print('Before feature executed')


# before every scenario
def before_scenario(scenario, context):
    print('Before scenario executed')
# after every feature
def after_feature(feature, context):
    print('After feature executed')

# after all
def after_all(context):
    print('After all executed')