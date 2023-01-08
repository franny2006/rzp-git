import io
import os
from cls_db import cls_dbAktionen

class cls_create_featureFiles():
    def __init__(self):
        # Bestehende Feature-Files löschen

        dir = '../features'
        for f in os.listdir(dir):
            try:
                if f.split(".")[1] == 'feature':
                    print(f)
                    os.remove(os.path.join(dir, f))
            except:
                pass


        self.db = cls_dbAktionen()
        auftraege = self.readAuftraege()
        for dictAuftrag in auftraege:
            self.createFile(dictAuftrag)
    #    self.createFile()
        pass

    def readAuftraege(self):
        sql = "select panr, prnr, voat, lfdNr, transaktionsId from transaktionIds where anzFehler = 0"
        auftraege = self.db.execSelect(sql, '')
        return auftraege

    def readMapping(self):
        sql = "select * from gherkin_mapping"
        mappings = self.db.execSelect(sql, '')
        return mappings

    def createFile(self, dictAuftrag):
        line = []
        line.append("Feature: Pruefe Adresse - Transaktion: " + str(dictAuftrag['transaktionsId']))
        line.append("  Scenario Outline: Adressen pruefen SZ")


        line.append("    Given Es wurde ein Auftrag mit PANR = " + str(dictAuftrag['panr']) + ", PRNR = " + str(dictAuftrag['prnr']) + ", VOAT = " + str(dictAuftrag['voat']) + ", lfdNr = " + str(dictAuftrag['lfdNr']) + ", TransaktionsId = " + str(dictAuftrag['transaktionsId']) + " eingespielt")
        line.append("    When dieser Auftrag vollstaendig verarbeitet wurde")
        line.append("    Then enthaelt in der Datenbank <zielDb> das Feld <zielFeld> den gleichen Inhalt wie <feldAuftrag>")

        line.append("")

        line.append("    Examples:")
        headerExampleTabelle = "    | " + str("feldAuftrag").ljust(50, ' ') + "| " + str("zielDb").ljust(50, ' ') + "| " + str("zielFeld").ljust(50, ' ') + "|"
        line.append(headerExampleTabelle)


        mappingRegeln = self.readMapping()
        for regel in mappingRegeln:
            feldAuftrag = regel['feldAuftrag'].ljust(50, ' ')
            feldZielDb = regel['zielDb'].ljust(50, ' ')
            feldZielFeld = regel['zielFeld'].ljust(50, ' ')
            line.append("    | " + feldAuftrag + "| " + feldZielDb + "| " + feldZielFeld + "|")



   #     line.append("    | SA_11.zunameZUNAME                       | KUGA.Anliegen          | keyValue.zahlungsempfaengerName.zunameZUNAME          |")
   #     line.append("    | SA_11.vornameVORNAME                     | KUGA.Anliegen          | keyValue.zahlungsempfaengerName.vornameVORNAME        |")

        f = io.open('../features/' + dictAuftrag['transaktionsId'] + '.feature', 'w', encoding='UTF-8')
        for zeile in line:
            f.write(zeile + "\n")
        f.close()

if __name__ == "__main__":
    x = cls_create_featureFiles()
