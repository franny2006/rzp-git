import json

from .cls_db import cls_dbAktionen

class cls_readCollectionsToInit():
    def __init__(self, file):
        with open(file) as json_file:
            self.db = cls_dbAktionen()
            self.truncateTable("init_collections")
            self.collectionsToInit = json.load(json_file)
            print(self.collectionsToInit)
            for collection in self.collectionsToInit['collectionsToInit']:
                print(collection['connectionName'], collection['collection'])
                parameterList = [collection['collection'], collection['connectionName']]
                sql = "insert into init_collections (collection, zielDb) values (%s, %s)"
                self.db.execSql(sql, parameterList)

    def truncateTable(self, table):
        sql = "truncate " + str(table)
        self.db.execSql(sql, '')


if __name__ == "__main__":
    x = cls_readCollectionsToInit()