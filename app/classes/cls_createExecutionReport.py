import json
import pandas as pd
import openpyxl



class cls_createExecutionReport():

    def __init__(self):
        file = 'export/reports/results.json'
        listTests = []
        featureNr = 1
        with open(file, encoding="utf8") as json_file:
            XmlReport = json.load(json_file)
            for feature in XmlReport:
                dictTest= {}
            #    print(szenarien)
                dictTest['featureNr'] = featureNr
                dictTest['featureName'] = feature['name']
                dictTest['featureStatus'] = feature['status']
            #    dictTest['featureKeyword'] = feature['keyword']


                dictTest['scenarioNr'] = 0
                dictTest['scenarioName'] = ""
                dictTest['scenarioStatus'] = ""
                for scenario in feature['elements']:
                    dictTest['scenarioNr'] = dictTest['scenarioNr'] + 1
                    dictTest['scenarioName'] = scenario['name']
                    dictTest['scenarioStatus'] = scenario['status']
            #        dictTest['scenarioKeyword'] = scenario['keyword']

                    dictTest['stepNr'] = 0
                    dictTest['stepKeyword'] = ""
                    dictTest['stepName'] = ""
                    dictTest['zielFeld'] = ""
                    dictTest['stepStatus'] = ""
                    dictTest['stepReasonFailure'] = ""
                    dictTest['ursprung'] = ""
                    dictTest['inhaltAuftrag'] = ""
                    dictTest['sollErgebnis konv.'] = ""
                    for step in scenario['steps']:
                        dictTest['stepNr'] = dictTest['stepNr'] + 1
                        dictTest['stepKeyword'] = step['keyword']
                        dictTest['stepName'] = step['name']
                        try:
                            dictTest['stepStatus'] = step['result']['status']
                        except:
                            dictTest['stepStatus'] = 'n.a.'
                        if dictTest['stepStatus'] == "failed":
                            dictTest['stepReasonFailure'] = step['result']['error_message']
                            if 'Verarbeitung in Datenbank unvollständig' in dictTest['stepReasonFailure']:
                                dictTest['stepStatus'] = "nicht in DB"
                        else:
                            dictTest['stepReasonFailure'] = "---"
                        try:
                            dictParameter = step['match']['arguments']
                            for parameter in dictParameter:
                                for key, value in parameter.items():
                                    if value == 'zielFeld':
                                        dictTest['zielFeld'] = parameter['value']
                                    if value == 'ursprung':
                                        dictTest['ursprung'] = parameter['value']
                                    if value == 'inhaltAuftrag':
                                        dictTest['inhaltAuftrag'] = parameter['value']
                                    if value == 'sollErgebnis':
                                        dictTest['sollErgebnis konv.'] = parameter['value']
                        except:
                            dictTest['zielFeld'] = ""
                            dictTest['ursprung'] = ""
                            dictTest['inhaltAuftrag'] = ""
                            dictTest['sollErgebnis konv.'] = ""

                        listTests.append(dict(dictTest))
                  #      print("DictTest:", dictTest)


                featureNr = featureNr + 1
   #             print("*********************************************************")

   #     print(listTests)
        filename = "export/reports/Testergebnisse.xlsx"
        pd.DataFrame(listTests).to_excel(filename)





if __name__ == "__main__":
    x = cls_createExecutionReport()