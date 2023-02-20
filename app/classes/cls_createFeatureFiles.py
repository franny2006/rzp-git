import io
import os
from .cls_db import cls_dbAktionen

#rzpDatenbanken = ['KUGA', 'AAN', 'WCH', 'PERF_IDENT', 'PERF_PERS', 'KAUS', 'REZA']

class cls_create_featureFiles():
    def __init__(self):
        # Bestehende Feature-Files löschen

    #    dirsToDelete = ['../features', '../export', '../export/reports', '../export/allure-results']
        dirsToDelete = ['features', 'export', 'export/reports', 'export/allure-results']
        for dir in dirsToDelete:
            for f in os.listdir(dir):
                try:
                    if f.split(".")[1] in ('feature', 'json'):
                     #   print(f)
                        os.remove(os.path.join(dir, f))
                except:
                    pass


        self.db = cls_dbAktionen()
        auftraege = self.readAuftraege()



        for dictAuftrag in auftraege:
            # Voraussetzung für die Anwendung einer Regel prüfen
            # Rollen:
            dictRegelnToExecute = self.readRegelnToExecute(dictAuftrag)


            self.line = []
            self.line.append("Feature: Pruefe Auftrag: "+ str(dictAuftrag['panr']) + " / " + str(dictAuftrag['prnr']) + " / " + str(dictAuftrag['voat']) + " - TId: " + str(dictAuftrag['transaktionsId']))

            self.createFile(dictAuftrag, dictRegelnToExecute)

       #     self.createFile_rollen(dictAuftrag)
    #    self.createFile()

    def readAuftraege(self):
       # sql = "select panr, prnr, voat, lfdNr, transaktionsId from transaktionIds where anzFehler = 0"
        sql = "select t.panr as t_panr, t.prnr as t_prnr, t.voat as t_voat, lfdNr, transaktionsId, zielwelt, v.* from transaktionIds t " \
              "left join V_DS10_komplett v " \
              "on v.panr = t.panr " \
              "and v.prnr = t.prnr " \
              "and v.voat = t.voat " \
              "and v.laufendeNummerZL = t.lfdNr " \
              "where anzFehler = 0 " \
              "and t.voat = 21"
        auftraege = self.db.execSelect(sql, '')
        return auftraege

    def readMapping(self, rzpDb, bedingung):
        if bedingung == "":
            sql = "select * from gherkin_mapping where bedingung = '' and zielDb = '" + rzpDb + "'"
        else:
            sql = "select * from gherkin_mapping where bedingung = '" + bedingung + "' and zielDb = '" + rzpDb + "'"
        mappings = self.db.execSelect(sql, '')
        return mappings

    def readRegelnToExecute(self, dictAuftrag):
        dictRegelnToExecute = ['',]
        # BE <> ZE
        if dictAuftrag['sa_19_dsId'] != None:
            dictRegelnToExecute.append('BE != ZE')
        else:
            dictRegelnToExecute.append('BE == ZE')

        # Mitteilungsempfänger
        if dictAuftrag['sa_m1_dsId'] != None:
            dictRegelnToExecute.append('SAM1')
        else:
            if dictAuftrag['sa_19_dsId'] != None:
                dictRegelnToExecute.append('keine SAM1 && BE != ZE')
            else:
                dictRegelnToExecute.append('keine SAM1 && BE == ZE')

        # Kontoinhaber
        if dictAuftrag['sa_17_dsId'] != None:
            dictRegelnToExecute.append('SA17')
        else:
            dictRegelnToExecute.append('keine SA17 && BE == ZE')

        print("DictRegeln: ", dictRegelnToExecute)
        return dictRegelnToExecute

    def createFile(self, dictAuftrag, bedingungen):

        if dictAuftrag['zielwelt'] == 'ALT':
            sql = "select rzpDb from rzp_datenbanken where rzpDb like 'KUGA%' OR rzpDb like 'AAN%' or rzpDb like 'WCH%' order by sort"
        else:
            sql = "select rzpDb from rzp_datenbanken order by sort"
        rzpDatenbanken = self.db.execSelect(sql, '')

        for rzpDb in rzpDatenbanken:
            self.line.append("  Scenario Outline: " + str(dictAuftrag['panr']) + " / "  + str(dictAuftrag['prnr']) + " / "  + str(dictAuftrag['voat']) + " in " + rzpDb['rzpDb'] + " pruefen")


            self.line.append("    Given Es wurde ein Auftrag mit PANR = " + str(dictAuftrag['panr']) + ", PRNR = " + str(dictAuftrag['prnr']) + ", VOAT = " + str(dictAuftrag['voat']) + ", lfdNr = " + str(dictAuftrag['lfdNr']) + ", TransaktionsId = " + str(dictAuftrag['transaktionsId']) + " eingespielt")
            self.line.append("    When dieser Auftrag in der Datenbank " + str(rzpDb['rzpDb']) + " gespeichert wurde")
            self.line.append("    Then enthaelt die Datenbank " + str(rzpDb['rzpDb']) + " im Feld <zielFeld> den SOLL-Wert '<Soll-Ergebnis>'. Urspruenglicher Auftragswert: '<inhaltAuftrag>' aus Feld '<ursprung>'")

            self.line.append("")

            self.line.append("    Examples:")
            headerExampleTabelle = "    | " + str("zielFeld").ljust(60, ' ') + "| " + str("inhaltAuftrag").ljust(50, ' ') + "| " + str("Soll-Ergebnis").ljust(50, ' ') + "| " + str("ursprung").ljust(70, ' ') + "| " + str("regel").ljust(20, ' ') + "| " + str("bedingung").ljust(20, ' ') + "|"
            self.line.append(headerExampleTabelle)

            for bedingung in bedingungen:
                pruefungen = self.readMapping(rzpDb['rzpDb'], bedingung)
                for pruefung in pruefungen:
                    feldInhaltAuftrag = ""
                    # Regeln, die vor der Verarbeitung ausgeführ werden müssen
                    if pruefung['regel'].lower() == "pre_concat":
                        feld1 = pruefung['feldAuftrag'].split("+")[0].rstrip()
                        feld2 = pruefung['feldAuftrag'].split("+")[1].lstrip()
                        feldInhaltAuftrag1 = self.ermittle_inhaltAuftrag(dictAuftrag, feld1)
                        feldInhaltAuftrag2 = self.ermittle_inhaltAuftrag(dictAuftrag, feld2)
                        feldInhaltAuftrag = feldInhaltAuftrag1.strip() + " " + feldInhaltAuftrag2.strip()
                    else:
                        # Wenn konkreter Wert geprüft werden soll (Präfix konkret.)
                        if pruefung['feldAuftrag'].split(".")[0] == "konkret":
                            feldInhaltAuftrag = pruefung['feldAuftrag'].split(".")[1]
                        else:
                            try:        # konkreter Wert des Auftrags ermitteln
                                feldInhaltAuftrag = self.ermittle_inhaltAuftrag(dictAuftrag, pruefung['feldAuftrag'])
                                if feldInhaltAuftrag is None:
                                    feldInhaltAuftrag = "Feld nicht vorhanden"
                                elif feldInhaltAuftrag.strip() == "":
                                    feldInhaltAuftrag = "<leer>"
                            except:
                                print("hier stimmt was nicht",  pruefung['feldAuftrag'])


                    if feldInhaltAuftrag:
                        if pruefung['regel'] != "-" or pruefung['regel'] != "" or pruefung['regel'][:3].lower() == "pre":
                            feldInhaltZiel = self.konvertierungsregel_anwenden(pruefung['regel'], feldInhaltAuftrag.strip())
                        else:
                            feldInhaltZiel = feldInhaltAuftrag.strip()
                        feldInhaltAuftrag = feldInhaltAuftrag.ljust(50)
                        feldZielFeld = pruefung['zielFeld'].ljust(60, ' ')
                        feldInhaltZiel = feldInhaltZiel.ljust(50, ' ')
                        feldInhaltUrsprung = pruefung['feldAuftrag'].ljust(70, ' ')
                        regel = pruefung['regel'].ljust(20, ' ')
                        bedingung = bedingung.ljust(20, ' ')
                        self.line.append("    | " + feldZielFeld + "| " + feldInhaltAuftrag + "| " + feldInhaltZiel + "| " + feldInhaltUrsprung + "| " + regel + "| " + bedingung + "| ")

            self.line.append("")
            self.line.append("")


        #    linesScenarioRollen =  self.createFile_rollen(dictAuftrag)
        #    line = self.line + linesScenarioRollen



        f = io.open('features/standard_' + dictAuftrag['transaktionsId'] + '.feature', 'w', encoding='UTF-8')
        for zeile in self.line:
            f.write(zeile + "\n")
        f.close()

    def createFile_rollen(self, dictAuftrag):
        line = []
        line.append("")
        line.append("")
        line.append("  Scenario Outline: Rollen pruefen")

        line.append("    Given Es wurde ein Auftrag mit PANR = " + str(dictAuftrag['panr']) + ", PRNR = " + str(dictAuftrag['prnr']) + ", VOAT = " + str(dictAuftrag['voat']) + ", lfdNr = " + str(dictAuftrag['lfdNr']) + ", TransaktionsId = " + str(dictAuftrag['transaktionsId']) + " eingespielt")
        line.append("    Then wurde dieser Auftrag vollstaendig verarbeitet")
        line.append("    And enthaelt die Rolle <rolle> in der Datenbank <zielDb> zum Auftragswert <inhaltAuftrag> im Feld <zielFeld> den SOLL-Wert <Soll-Ergebnis>")

        line.append("")

        line.append("    Examples:")
        headerExampleTabelle = "    | " + str("rolle").ljust(30, ' ') + "| " + str("zielDb").ljust(30, ' ') + "| " + str("zielFeld").ljust(60, ' ') + "| " + str("inhaltAuftrag").ljust(50, ' ') + "| " + str("Soll-Ergebnis").ljust(50, ' ') + "| " + str("regel").ljust(20, ' ') + "|"
        line.append(headerExampleTabelle)

        pruefungen = self.readMapping("rollen")
        for pruefung in pruefungen:
            try:
                feldInhaltAuftrag = self.ermittle_inhaltAuftrag(dictAuftrag, pruefung['feldAuftrag'])
                if feldInhaltAuftrag.strip() == "":
                    feldInhaltAuftrag = "<leer>"
            except:
                print("hier stimmt was nicht", feldInhaltAuftrag)

            if feldInhaltAuftrag:
                if pruefung['regel'] != "-" or pruefung['regel'] != "":
                    feldInhaltZiel = self.konvertierungsregel_anwenden(pruefung['regel'], feldInhaltAuftrag.strip())
                else:
                    feldInhaltZiel = feldInhaltAuftrag.strip()
                feldInhaltAuftrag = feldInhaltAuftrag.ljust(50)

                feldZielDb = pruefung['zielDb'].ljust(30, ' ')
                feldZielFeld = pruefung['zielFeld'].ljust(60, ' ')
                feldInhaltZiel = feldInhaltZiel.ljust(50, ' ')
                rolle = pruefung['rolle'].ljust(30, ' ')
                regel = pruefung['regel'].ljust(20, ' ')
                line.append("    | " + rolle + "| " + feldZielDb + "| " + feldZielFeld + "| " + feldInhaltAuftrag + "| " + feldInhaltZiel + "| " + regel + "|")

        return line

    def ermittle_inhaltAuftrag(self, auftrag, zielfeld):
        listZielfeld = zielfeld.split(".")
        feldAuftrag = listZielfeld[0].lower() + "_" + listZielfeld[1]

        inhaltAuftrag = auftrag[feldAuftrag]
     #   print("FeldAuftrag:", feldAuftrag, inhaltAuftrag)
        return inhaltAuftrag

    def konvertierungsregel_anwenden(self, regel, inhaltAuftrag):
        from datetime import datetime
        if regel == "datum":
            inhaltAuftrag = datetime.strptime(inhaltAuftrag, '%Y%m%d').date()
        elif regel.lower() == "datum_yyyymm":
            dataTemp = datetime.strptime(inhaltAuftrag, '%Y%m').date()
            inhaltAuftrag = dataTemp.strftime('%Y-%m')
        elif regel[:3].lower() == 'ps_':
            feldAuftrag = regel[3:]
            sql = "select distinct keyRzp from schluessel where feldAuftrag = '" + str(feldAuftrag) + "' and keyAuftrag = '" + str(inhaltAuftrag) + "'"
            valueZiel = self.db.execSelect(sql, '')
            try:
                inhaltAuftrag = valueZiel[0]['keyRzp'].upper()
            except:
                inhaltAuftrag = "kein Mapping vorhanden für Wert " + str(inhaltAuftrag) + " in Feld " + str(feldAuftrag)
      #      print(inhaltAuftrag)

        return (str(inhaltAuftrag))

if __name__ == "__main__":
    x = cls_create_featureFiles()
