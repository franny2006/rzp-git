from .cls_readSchema import cls_readSchema
from .cls_db import cls_dbAktionen
from configparser import ConfigParser
config = ConfigParser()
config.read('../config/config.ini')


class cls_createSchema():
    def __init__(self):
        herkunft = "testdaten_leer"  # leer, testdatenEkl, testdatenEkl_Prod, testdaten_GIT, testdaten_GIT2
        self.db = cls_dbAktionen(herkunft)
        self.createTableRuns()
        self.createTableTransaktionsId()
        self.createTableDocuments()
  #     self.createTableRollen()
        self.createTables()
        self.createView()
        self.createTableGherkin_Mappings()
        self.createTablInitCollections()
        self.createTableSchluessel()
        self.createTableRzpDatenbanken()



    def createTableRuns(self):
        sql = "drop table if exists runs"
        self.db.execSql(sql, '')
        sql = "CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, datei varchar(255), sendungsnummer varchar(10), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
        self.db.execSql(sql, '')

    def createTableTransaktionsId(self):
        sql = "drop table if exists transaktionIds"
        self.db.execSql(sql, '')
        sql = "CREATE TABLE IF NOT EXISTS transaktionIds " \
              "(datei varchar(150), sendungsnummer varchar(10), panr varchar(4), prnr varchar(14), voat varchar(2), lfdNr varchar(8), " \
              "transaktionsId varchar(36), identitaetenId_ze varchar(36), personId_ze varchar(36), identitaetenId_be varchar(36), personId_be varchar(36), " \
              "identitaetenId_me varchar(36), personId_me varchar(36), identitaetenId_ki varchar(36), personId_ki varchar(36), " \
              "pruefergebnis varchar(50), anzHinweise varchar(5), hinweis varchar(5), anzFehler varchar(5), fehler varchar(5), zielwelt varchar(10), " \
              "kuga char(1), kuga_status varchar(255), aan char(1), aan_status varchar(255), wch char(1), wch_status varchar(255), " \
              "perf_ident char(1), perf_ident_status varchar(255), perf_pers char(1), perf_pers_status varchar(255), refue char(1), refue_status varchar(255), " \
              "reza char(1), reza_status varchar(255), kaus char(1), kaus_status varchar(255), " \
              "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " \
              "PRIMARY KEY (panr, prnr, voat, lfdNr))"
        self.db.execSql(sql, '')

    def createTableDocuments(self):
        sql = "drop table if exists documents"
        self.db.execSql(sql, '')
        sql = "CREATE TABLE documents (herkunft varchar(100) CHARACTER SET utf8mb4 NOT NULL," \
              "transaktionsId varchar(36) CHARACTER SET utf8mb4 NOT NULL, rolle char(2) DEFAULT '' not null, identitaetenId varchar(36) DEFAULT '' not null, personId varchar(36) DEFAULT '' not null, " \
              "document JSON NOT NULL, " \
              "ts timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()," \
              "PRIMARY KEY (transaktionsId, herkunft, rolle, identitaetenId, personId)) " \
              "ENGINE=InnoDB DEFAULT CHARSET=latin1;"
        self.db.execSql(sql, '')

    def createTableRzpDatenbanken(self):
        sql = "drop table if exists rzp_datenbanken"
        self.db.execSql(sql, '')
        sql = "CREATE TABLE IF NOT EXISTS rzp_datenbanken " \
              "(id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, rzpDB varchar(70), sort INTEGER, feldIdentifizierung_1 varchar(100), feldIdentifizierung_2 varchar(100), feldIdentifizierung_3 varchar(100), feldSortierung varchar(100), feldStatus varchar(100))"
        self.db.execSql(sql, '')
        sqlInsertList = ["insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldSortierung, feldStatus) values ('KUGA.Anliegen', 1, '_id', '_id', 'statushistorie')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldSortierung, feldStatus) values ('AAN.order', 2, 'transaktionsId', 'sequenz', 'metadaten.statushistorie')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldSortierung, feldStatus) values ('WCH.anliegen', 3, 'payload.transaktionsId', 'kontext.markierung', 'statusHistorie')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldIdentifizierung_2, feldSortierung, feldStatus) values ('PERF_IDENT.identitaeten', 4, 'transaktionsId', 'identitaetenId', '_id', 'fachlicherStatus')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldIdentifizierung_2, feldSortierung, feldStatus) values ('PERF_PERS.personen', 5, 'transaktionsId', 'personId', '_id', 'fachlicherStatus')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldSortierung, feldStatus) values ('REFUE.LaufendeGeldleistung', 6, 'transaktionsId', '_id', 'fachlicherStatus')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldSortierung, feldStatus) values ('REZA.Geldleistungskonten', 7, 'transaktionsId', '_id', 'fachlicherStatus')",
                         "insert into rzp_datenbanken (rzpDb, sort, feldIdentifizierung_1, feldSortierung, feldStatus) values ('KAUS.Kundeninformation', 8, 'transaktionsId', '_id', 'kundeninformationStatusListe')"]
        for sqlInsert in sqlInsertList:
            self.db.execSql(sqlInsert, '')

    def createTablInitCollections(self):
        sql = "CREATE TABLE IF NOT EXISTS init_collections (id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, collection varchar(255), zielDb varchar(255), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
        self.db.execSql(sql, '')
        sqlInsertList = ["insert into init_collections (collection, zielDb) values ('test', 'AAN')",
                         "insert into init_collections (collection, zielDb) values ('test2', 'TransaktionsId')"]
        for sqlInsert in sqlInsertList:
            self.db.execSql(sqlInsert, '')

    def createTableRollen(self):
        sql = "drop table if exists rollen"
        self.db.execSql(sql, '')
        sql = "CREATE TABLE IF NOT EXISTS rollen " \
              "(transaktionsId varchar(36), id varchar(36), art varchar(20), abweichend varchar(50), anrede varchar(50), zuname varchar(50), vorname varchar(50), strasse varchar(50), hausnummer varchar(50), plz varchar(50), ort varchar(50), " \
              "land varchar(50), geburtsdatum varchar(50), kommunikationsmerkmal varchar(50), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, " \
              "PRIMARY KEY (transaktionsId, id, art))"
        self.db.execSql(sql, '')

    def createTableGherkin_Mappings(self):
        sql = "CREATE TABLE IF NOT EXISTS gherkin_mapping (id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, feldAuftrag varchar(255), zielDb varchar(255), zielFeld varchar(255), rolle char(2), regel varchar(255), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
        self.db.execSql(sql, '')



    def createTableSchluessel(self):
        sql = "drop table if exists schluessel"
        self.db.execSql(sql, '')
        sql = "CREATE TABLE IF NOT EXISTS schluessel " \
              "(id INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, feldAuftrag varchar(100), keyAuftrag varchar(100), keyRzp varchar(100), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"

        self.db.execSql(sql, '')
        sqlInsertList = ["insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '1', 'Herr')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '2', 'Frau')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '3', 'Fräulein')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '4', 'Damen und Herren')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '5', 'Herren')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '6', 'Damen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('anrede', '7', 'Guten Tag')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '00', 'Grundstellung_Normaldruck')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '01', 'Großdruck')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '02', 'Braille_Kurzschrift')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '03', 'Braille_Langschrift')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '12', 'Bereitstellung als E-Mail')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '13', 'CD-ROM_Schriftdatei')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('mmbarko', '22', 'Hörmedium_CD-ROM_DAISY')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('zahlzeitraumZLZR', '1', 'monatlich')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('zahlzeitraumZLZR', '3', 'vierteljährlich')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('zahlzeitraumZLZR', '6', 'halbjährlich')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('zahlzeitraumZLZR', '9', 'jährlich')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '14', 'Rente_wegen_teilweiser_Erwerbsminderung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '15', 'Rente_wegen_voller_Erwerbsminderung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '16', 'Regelaltersrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '17', 'Altersrente_wegen_Arbeitslosigkeit_oder_nach_Altersteilzeitarbeit')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '18', 'Altersrente_fuer_Frauen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '20', 'Kleine_Witwenrente_oder_kleine_Witwerrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '21', 'Große_Witwenrente_oder_große_Witwerrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '22', 'Witwen-/Witwerrentenabfindung_einer_Rente_der_Leistungsart_20')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '23', 'Witwen-/Witwerrentenabfindung_einer_Rente_der_Leistungsart_21')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '25', 'Halbwaisenrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '26', 'Vollwaisenrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '30', 'Beitragserstattung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '31', 'Altersrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '32', 'Invalidenrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '33', 'Invalidenrente_fuer_Behinderte')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '35', 'Witwenrente_oder_Witwerrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '36', 'Uebergangshinterbliebenenrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '37', 'Unterhaltsrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '38', 'Halbwaisenrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '39', 'Vollwaisenrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '43', 'Rente_wegen_voller_Erwerbsminderung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '45', 'Erziehungsrente')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '46', 'Leistung_fuer_Kindererziehung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '62', 'Altersrente_fuer_schwerbehinderte_Menschen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '63', 'Altersrente_fuer_langjaehrig_Versicherte')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '65', 'Altersrente_fuer_besonders_langjaehrig_Versicherte')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '74', 'Rente_wegen_teilweiser_Erwerbsminderung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '75', 'Rente_wegen_voller_Erwerbsminderung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '76', 'Rente_wegen_voller_Erwerbsminderung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '95', 'Zinsen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '96', 'Erstattungen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '97', 'Verrechnung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '99', 'Bankspesen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('leistungsartLEAT', '99', 'Bankspesen')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('zahlterminZLTE', '0', 'vorschuessig')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('zahlterminZLTE', '1', 'nachschuessig')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('abgetrennteZahlungTLZL', '0', 'EINZELBETRAGSLEISTUNG')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('abgetrennteZahlungTLZL', '1', 'STAMMRENTE')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('abgetrennteZahlungTLZL', '5', 'ABGETRENNTER_TEIL_DER_ZAHLUNG')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalRentnerausweisMMRA', '0', 'RENTENAUSWEIS_AUSSTELLEN')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalRentnerausweisMMRA', '1', 'RENTENAUSWEIS_NICHT_AUSSTELLEN')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalKostenabzugMMKO', '0', 'GRUNDSTELLUNG_ODER_KEIN_KOSTENABZUG')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalKostenabzugMMKO', '1', 'KOSTENABZUG_FUER_EU_SCHECKZAHLUNGEN')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalKostenabzugMMKO', '2', '')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalKostenabzugMMKO', '3', 'Kostenabzug für Zahlungsanweisungen zur Verrechnung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalKostenabzugMMKO', '4', 'Rueckwirkende Kostenerstattung')",
                         "insert into schluessel (feldAuftrag, keyAuftrag, keyRzp) values ('merkmalKostenabzugMMKO', '5', 'Anwendung Haertefallregelung')"]
        for sqlInsert in sqlInsertList:
           # print(sqlInsert)
            self.db.execSql(sqlInsert, '')

    def createTables(self):
        schema = cls_readSchema()
        listSatzarten = schema.returnSatzarten()
        for sa in listSatzarten:
            print("********** ", str(sa).lower(), " ***************")
            self.dropTable(str(sa).lower())

            sql = "CREATE TABLE IF NOT EXISTS sa_" + str(sa).lower() + " (runId INTEGER, dsId INTEGER, "

            listSatzartFelder = schema.returnSatzartenFelder(sa)
            for feld in listSatzartFelder:
                sql = sql + feld + ' varchar(255), '
            sql = sql[:-2] + ");"

            print(sql)
            self.db.execSql(sql, '')
            # print(listSatzartFelder)

    def dropTable(self, sa):
        sql = "drop table if exists sa_" + sa
        self.db.execSql(sql, '')


    def createView(self):
        schema = cls_readSchema()
        listSatzarten = schema.returnSatzarten()
        prefix = "SA"

        viewName = "V_DS10_komplett"
        print(viewName)

        sqlCreateView = "CREATE OR REPLACE VIEW " + viewName + " AS SELECT * FROM sa_ft ft"

        i = 0
        for sa in listSatzarten:
            sa = "sa_" + str(sa).lower()
            sqlGetColumns = "SHOW COLUMNS FROM " + sa
            columns = self.db.execSelect(sqlGetColumns, '')
            selectJoin = "(SELECT "
            for column in columns:
                selectJoin = selectJoin + " " + column['Field'] + " AS " + sa + "_" + column['Field'][:58] + ","
            selectJoin = selectJoin[
                         :-1] + " FROM " + sa + ") " + sa + " on ft.runId = " + sa + "." + sa + "_runId and ft.dsId = " + sa + "." + sa + "_dsId "
            print(selectJoin)

            if sa != "beschreibung" and sa != "id":
                print("Treffer für SA: ", sa)
                # sqlCreateView = sqlCreateView + " left join " + prefix + "_" + saToView + " on ft.runId = " + prefix + "_" + saToView + ".runId and ft.dsId = " + prefix + "_" + saToView + ".dsId "
                sqlCreateView = sqlCreateView + " left join " + selectJoin

            i = i + 1
        print(sqlCreateView)
        self.db.execSql(sqlCreateView, '')

    def reset(self):
        tablesToDelete = []
        tablesToDelete.append("select \'drop table \' || name || \';\' from sqlite_master where type = 'table';")

        db = cls_dbAktionen()

        statements = []
        for tableset in tablesToDelete:
            print("Tableset: ", tableset)
            result = db.execSelect(tableset, '')
            for statement in result:
                statements.append(statement)
            for a in statements:
                print(a[0])
                try:
                    db.execSql(a[0], '')
                except:
                    print("hat nicht funktioniert")

        db.closeDB()

    def tabelleninhalte_loeschen(self):

        sql = "select \'delete from \' || name || \';\' " \
                    "from sqlite_master " \
                    "where type = 'table' "\
                    "and name not in ('fehlercodes', 'hinweiscodes')"

        statements = []
        result = self.db.execSelect(sql, '')
        for statement in result:
            statements.append(statement)
        for a in statements:
            print(a[0])
            try:
                self.db.execSql(a[0], '')
            except:
                print("hat nicht funktioniert")

        self.db.closeDB()



    # deprecated



if __name__ == "__main__":
    x = cls_createSchema()