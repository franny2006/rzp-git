from cls_db import cls_dbAktionen

class cls_test():
    def __init__(self):
        self.db = cls_dbAktionen()
        self.test()

    def test(self):
        createTable = "Create table IF NOT EXISTS TEST (feld varchar(100), bereich varchar(100), AUFTRAG varchar(100), KUGA varchar(100), WCH varchar(100), KAUS varchar(100)"
        selectInsert = "insert into TEST set (feld, auftrag) values "
        sqlGetColumns = "SHOW COLUMNS FROM v_ds10_komplett"
        columns = self.db.execSelect(sqlGetColumns, '')
        for column in columns:
            selectInsert ="insert into TEST set (feld, auftrag) values '" + column['Field'] + "', '" + column['Field'] + "')"

            print(selectInsert)



if __name__ == "__main__":
    x = cls_test()