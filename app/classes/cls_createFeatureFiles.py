import io
import os
from .cls_db import cls_dbAktionen

class cls_create_featureFiles():
    def __init__(self):
        # Bestehende Feature-Files löschen

        dirsToDelete = ['features', 'export', 'export/reports', 'export/allure-results']
        for dir in dirsToDelete:
            for f in os.listdir(dir):
                try:
                    if f.split(".")[1] in ('feature', 'json'):
                        print(f)
                        os.remove(os.path.join(dir, f))
                except:
                    pass


        self.db = cls_dbAktionen()
        auftraege = self.readAuftraege()
        for dictAuftrag in auftraege:
            self.createFile_standard(dictAuftrag)
            self.createFile_rollen(dictAuftrag)
    #    self.createFile()
        pass

    def readAuftraege(self):
       # sql = "select panr, prnr, voat, lfdNr, transaktionsId from transaktionIds where anzFehler = 0"
        sql = "select t.panr as t_panr, t.prnr as t_prnr, t.voat as t_voat, lfdNr, transaktionsId, v.* from transaktionIds t " \
              "left join V_DS10_komplett v " \
              "on v.panr = t.panr " \
              "and v.prnr = t.prnr " \
              "and v.voat = t.voat " \
              "and v.laufendeNummerZL = t.lfdNr " \
              "where anzFehler = 0"
        auftraege = self.db.execSelect(sql, '')
        return auftraege

    def readMapping(self, art):
        if art == "standard":
            sql = "select * from gherkin_mapping where rolle is Null"
        elif art == "rollen":
            sql = "select * from gherkin_mapping where rolle is not Null"
        mappings = self.db.execSelect(sql, '')
        return mappings

    def createFile_standard(self, dictAuftrag):
        line = []
        line.append("Feature: Pruefe Einzelfelder: " + str(dictAuftrag['transaktionsId']))
        line.append("  Scenario Outline: Einzelne Felder pruefen")


        line.append("    Given Es wurde ein Auftrag mit PANR = " + str(dictAuftrag['panr']) + ", PRNR = " + str(dictAuftrag['prnr']) + ", VOAT = " + str(dictAuftrag['voat']) + ", lfdNr = " + str(dictAuftrag['lfdNr']) + ", TransaktionsId = " + str(dictAuftrag['transaktionsId']) + " eingespielt")
        line.append("    When dieser Auftrag vollstaendig verarbeitet wurde")
        line.append("    Then enthaelt in der Datenbank <zielDb> das Feld <zielFeld> den ggf. nach Regel <regel> konvertierten Wert <inhaltAuftrag>")

        line.append("")

        line.append("    Examples:")
        headerExampleTabelle = "    | " + str("zielDb").ljust(50, ' ') + "| " + str("zielFeld").ljust(60, ' ') + "| " + str("inhaltAuftrag").ljust(120, ' ') + "| " + str("regel").ljust(60, ' ') + "|"
        line.append(headerExampleTabelle)


        mappingRegeln = self.readMapping("standard")
        for regel in mappingRegeln:
            feldInhaltAuftrag = self.ermittle_inhaltAuftrag(dictAuftrag, regel['feldAuftrag']).ljust(120)

            feldZielDb = regel['zielDb'].ljust(50, ' ')
            feldZielFeld = regel['zielFeld'].ljust(60, ' ')
            regel = regel['regel'].ljust(50, ' ')
            line.append("    | " + feldZielDb + "| " + feldZielFeld + "| " + feldInhaltAuftrag + "| " + regel + "|")


        f = io.open('features/standard_' + dictAuftrag['transaktionsId'] + '.feature', 'w', encoding='UTF-8')
        for zeile in line:
            f.write(zeile + "\n")
        f.close()

    def createFile_rollen(self, dictAuftrag):
        line = []
        line.append("Feature: Pruefe Rollen - Transaktion: " + str(dictAuftrag['transaktionsId']))
        line.append("  Scenario Outline: Rollen pruefen")

        line.append("    Given Es wurde ein Auftrag mit PANR = " + str(dictAuftrag['panr']) + ", PRNR = " + str(dictAuftrag['prnr']) + ", VOAT = " + str(dictAuftrag['voat']) + ", lfdNr = " + str(dictAuftrag['lfdNr']) + ", TransaktionsId = " + str(dictAuftrag['transaktionsId']) + " eingespielt")
        line.append("    When dieser Auftrag vollstaendig verarbeitet wurde")
        line.append("    Then enthaelt zur Rolle <rolle> in der Datenbank <zielDb> das Feld <zielFeld> den ggf. nach Regel <regel> konvertierten Wert <inhaltAuftrag>")

        line.append("")

        line.append("    Examples:")
        headerExampleTabelle = "    | " + str("inhaltAuftrag").ljust(120, ' ') + "| " + str("zielDb").ljust(50, ' ') + "| " + str("rolle").ljust(6, ' ') + "| " + str("zielFeld").ljust(60, ' ') + "| " + str("regel").ljust(60, ' ') + "|"
        line.append(headerExampleTabelle)

        mappingRegeln = self.readMapping("rollen")
        for regel in mappingRegeln:
            feldInhaltAuftrag = self.ermittle_inhaltAuftrag(dictAuftrag, regel['feldAuftrag']).ljust(120)
            #       print("Feldinhalt: " + feldInhaltAuftrag)

            feldZielDb = regel['zielDb'].ljust(50, ' ')
            feldZielFeld = regel['zielFeld'].ljust(60, ' ')
            rolle = regel['rolle'].ljust(6, ' ')
            regel = regel['regel'].ljust(50, ' ')
            line.append("    | " + feldInhaltAuftrag + "| " + feldZielDb + "| " + rolle + "| " + feldZielFeld + "| " + regel + "|")

        f = io.open('features/rollen_' + dictAuftrag['transaktionsId'] + '.feature', 'w', encoding='UTF-8')
        for zeile in line:
            f.write(zeile + "\n")
        f.close()

    def ermittle_inhaltAuftrag(self, auftrag, zielfeld):
        listZielfeld = zielfeld.split(".")
        feldAuftrag = listZielfeld[0].lower() + "_" + listZielfeld[1]
        inhaltAuftrag = auftrag[feldAuftrag]
        print(inhaltAuftrag)
        return inhaltAuftrag

if __name__ == "__main__":
    x = cls_create_featureFiles()
