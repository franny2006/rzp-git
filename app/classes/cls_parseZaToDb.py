import json
import os
from .cls_readSchema import cls_readSchema
from .cls_db import cls_dbAktionen


class cls_parseZa():
    def __init__(self, filepath):
        print("Filepath:", filepath)
        file = filepath
    #    file = filedialog.askopenfilename()
        self.schema = cls_readSchema()
        herkunft = "testdaten_temp"   # leer, testdatenEkl, testdatenEkl_Prod, testdaten_GIT, testdaten_GIT2, testdaten_temp
        self.db = cls_dbAktionen(herkunft)

        # Run anlegen
        insertRun = "insert into runs (datei) values (%s)"
        filename = os.path.split(file)[1]
        print("Dateiname: ", filename)
        self.runId = self.db.execSql(insertRun, [filename,])
        self.dsId = 0
        datei = {}

        strEncoding = "iso-8859-1"

        with open(file, encoding=strEncoding) as ds10File:
            for index, line in enumerate(ds10File):
             #   try:
                    saFelder = {}

                 #   print("Line {}: {}".format(index, line.strip()))
                    position = 0
                    beitragssumme = 0
                    laengeZeile = len(line)
                    lineEnd = False
                    while lineEnd != True:
                        # Prüfung, ob DS10-Satz (kein BEN, Kopf- oder Fusssatz)
                        if line[0:3] not in ('VOS', 'NCS'):
                            if position == 0:
                                zeile = index + 1
                                if line[0:3] == 'BEN':
                                    sa = 'BEN'
                                    panr = line[12:16]
                                    prnr = line[16:30]
                                    voat = "BEN"
                                else:
                                    sa = "FT"
                                    panr = line[37:40]
                                    prnr = line[40:54]
                                    voat = line[35:37]
                                sql_valuelist = [self.runId, zeile, sa, panr, prnr, voat]
                                sql_zusammenfassung = "insert into za_datei (runId, zeile, art, panr, prnr, voat) values (%s, %s, %s, %s, %s, %s)"

                           #     self.db.execSql(sql_zusammenfassung, sql_valuelist)
                            elif position != 0 and position < laengeZeile-2:
                                sa = line[position:position + 2]
                            else:
                                lineEnd = True

                            if lineEnd != True:
                                dictSa = self.schema.returnSatzartenFelderKomplett(sa)
                                laengeSa = self.schema.laengeSa(sa)
                                stringSa = line[position:int(position) + int(laengeSa)]
                                saFelder[sa] = self.returnDictSatzart(sa, laengeSa, stringSa, dictSa)
                                position = int(position) + int(laengeSa)
                        else:
                            #print("kein Auftragssatz: ", line)
                            lineEnd = True

                    datei[str(index)] = saFelder
           #     except:
           #         print("Line {} kann nicht verarbeitet werden".format(index))
           #         pass


            for ds in datei.values():
                # print("ds: ", ds, type(ds))
                self.dsId = self.dsId + 1
             #  try:
                for sa, saInhalte in ds.items():
                #    print("SA: ", sa)
                    sql_felder = ""
                    sql_inhalte = ""
                    sql_params_platzhalter = ""
                    sqlValuesList = [self.runId, self.dsId]

                    for feld, feldinhalt in saInhalte.items():
                      #  print(feld, feldinhalt)
                        sql_felder = sql_felder + feld + ', '
                      #  sql_inhalte = sql_inhalte + '\'' + feldinhalt + '\', '
                        sqlValuesList.append(feldinhalt)
                        sql_params_platzhalter = sql_params_platzhalter + '%s, '
              #      sql = "insert into SA_" + str(sa) + ' (runId, dsId, ' + sql_felder[:-2] + ') values (' + str(self.runId) + ', ' + str(self.dsId) + ', ' + sql_inhalte[:-2] + ')'
                    sql = "insert into sa_" + str(sa).lower() + ' (runId, dsId, ' + sql_felder[:-2] + ') values (%s, %s, ' + sql_params_platzhalter[:-2] + ')'
                    print(sql)
                    self.db.execSql(sql, sqlValuesList)
                  #      sql_params = str(self.runId) + ', ' + str(self.dsId) + ', ' + sql_inhalte[:-2]
                  #      print(sql, sqlValuesList)
             #   except:
             #       print("Line {}: {}".format(index, line.strip()) + " kann nicht geschrieben werden")






    def returnDictSatzart(self, sa, laengeSa, stringSa, dictSa):
        saFelder = {}
        position = 0
     #   print("Satzart: ", sa, stringSa)
        while position < laengeSa:
            dictFeldlaenge = self.schema.returnFeld(sa, position)
            for key, feldlaenge in dictFeldlaenge.items():
                saFelder[key] = str(stringSa[position:int(position)+int(feldlaenge)])
                position = position + feldlaenge
        return saFelder





if __name__ == "__main__":
    x = cls_parseZa()