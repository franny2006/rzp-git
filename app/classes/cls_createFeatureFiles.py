import io
import os
from .cls_db import cls_dbAktionen

class cls_create_featureFiles():
    def __init__(self):
        # Bestehende Feature-Files löschen

        dirsToDelete = ['features', 'features/reports']
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
            self.createFile(dictAuftrag)
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
        line.append("    Then enthaelt in der Datenbank <zielDb> das Feld <zielFeld> den ggf. nach Regel <regel> konvertierten Wert <inhaltAuftrag>")

        line.append("")

        line.append("    Examples:")
        headerExampleTabelle = "    | " + str("zielDb").ljust(50, ' ') + "| " + str("zielFeld").ljust(60, ' ') + "| " + str("inhaltAuftrag").ljust(120, ' ') + "| " + str("regel").ljust(60, ' ') + "|"
        line.append(headerExampleTabelle)


        mappingRegeln = self.readMapping()
        for regel in mappingRegeln:
            feldInhaltAuftrag = self.ermittle_inhaltAuftrag(dictAuftrag, regel['feldAuftrag']).ljust(120)
     #       print("Feldinhalt: " + feldInhaltAuftrag)

            feldZielDb = regel['zielDb'].ljust(50, ' ')
            feldZielFeld = regel['zielFeld'].ljust(60, ' ')
            feldAuftrag = regel['feldAuftrag'].ljust(120, ' ')
            regel = regel['regel'].ljust(50, ' ')
            line.append("    | " + feldZielDb + "| " + feldZielFeld + "| " + feldInhaltAuftrag + "| " + regel + "|")



   #     line.append("    | SA_11.zunameZUNAME                       | KUGA.Anliegen          | keyValue.zahlungsempfaengerName.zunameZUNAME          |")
   #     line.append("    | SA_11.vornameVORNAME                     | KUGA.Anliegen          | keyValue.zahlungsempfaengerName.vornameVORNAME        |")

        f = io.open('features/' + dictAuftrag['transaktionsId'] + '.feature', 'w', encoding='UTF-8')
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
