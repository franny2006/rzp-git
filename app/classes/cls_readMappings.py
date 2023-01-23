import json

from .cls_db import cls_dbAktionen

class cls_readMappings():
    def __init__(self, file):
        with open(file) as json_file:
            self.db = cls_dbAktionen()
            self.truncateTable("gherkin_mapping")
            self.mappingData = json.load(json_file)
            print(self.mappingData)
            for regel in self.mappingData['mappings']:
                print(regel['feldAuftrag'], regel['zielDb'])
                parameterList = [regel['feldAuftrag'], regel['zielDb'], regel['zielFeld'], regel['rolle'], regel['regel']]
                sql = "insert into gherkin_mapping (feldAuftrag, zielDb, zielFeld, rolle, regel) values (%s, %s, %s, %s, %s)"
                self.db.execSql(sql, parameterList)

    def truncateTable(self, table):
        sql = "truncate " + str(table)
        self.db.execSql(sql, '')


if __name__ == "__main__":
    x = cls_readMappings()