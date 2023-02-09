import mysql.connector
import sys
import traceback
from configparser import ConfigParser

config = ConfigParser()
config.read('../config/config.ini')

class cls_dbAktionen():
    def __init__(self, zieldb=None):
        config = ConfigParser()
        config.read('config/config.ini')
        if config.get('Installation', 'zielumgebung').lower() == "local":
            ziel = 'mysql Datenbank local'
        else:
            ziel = 'mysql Datenbank docker'
        configuration = {
            'user': config.get(ziel, 'user'),
            'password': config.get(ziel, 'pass'),
            'host': config.get(ziel, 'host'),
            'port': config.get(ziel, 'port'),
            'database': config.get(ziel, 'database')
        }

        mydb = mysql.connector.connect(host=configuration['host'], user=configuration['user'], password=configuration['password'])
   #     mydb = mysql.connector.connect(host="rzp-mysql", user="root", password="root")
        self.mycursor = mydb.cursor()
        self.mycursor.execute("CREATE DATABASE IF NOT EXISTS rzp_git")


        self.mydb = mysql.connector.connect(host=configuration['host'], user=configuration['user'], password=configuration['password'], database=configuration['database'])
    #    self.mydb = mysql.connector.connect(host="rzp-mysql", user="root", password="root", database="rzp_git")
        mycursor = self.mydb.cursor()
        mycursor.execute("CREATE TABLE IF NOT EXISTS customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))")




    def execSql(self, statement, val):
        mycursor = self.mydb.cursor()
        try:
            if val:
                mycursor.execute(statement, val)
            else:
                mycursor.execute(statement)

            lastRowId = mycursor.lastrowid
            self.mydb.commit()

        except mysql.connector.Error as er:
            if self.mycursor:
#                print('MySql error: %s' % (' '.join(er.args)))
                print("Exception class is: ", er.__class__)
                print('MySql traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                print(traceback.format_exception(exc_type, exc_value, exc_tb))

        return lastRowId


    def execSelect(self, statement, val):
        mycursor = self.mydb.cursor(dictionary=True)
    #    mycursor.execute(statement)
    #    result = mycursor.fetchall()
        try:
   #         print(statement)
            if val:
                mycursor.execute(statement, val)
            else:
                mycursor.execute(statement)

            result = mycursor.fetchall()
            self.mydb.commit()
        except mysql.connector.Error as er:
            if mycursor:
                print('SQLite error: %s' % (' '.join(er.args)))
                print("Exception class is: ", er.__class__)
                print('SQLite traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                print(traceback.format_exception(exc_type, exc_value, exc_tb))

        return result

    def execSelectRowFactory(self, statement, val):
    #    print("SQL-Select: ", statement, val)

        try:
            cur = self.con.cursor()
            if val:
                cur.row_factory = lite.Row
                cur.execute(statement, val)
            else:
                cur.row_factory = lite.Row
                cur.execute(statement)

            result = cur.fetchall()
        except lite.Error as er:
            if self.con:
                self.con.rollback()
                print('SQLite error: %s' % (' '.join(er.args)))
                print("Exception class is: ", er.__class__)
                print('SQLite traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                print(traceback.format_exception(exc_type, exc_value, exc_tb))

        return result

    def execSelectDf(self, statement, val):
        print("SQL-Select: ", statement, val)

        try:
            cur = self.con.cursor()
            if val:
                cur.row_factory = lite.Row
                # cur.execute(statement, val)
                result = pd.read_sql(statement, self.con)
            else:
                cur.row_factory = lite.Row
                # cur.execute(statement)
                result = pd.read_sql(statement, self.con)

        except lite.Error as er:
            if self.con:
                self.con.rollback()
                print('SQLite error: %s' % (' '.join(er.args)))
                print("Exception class is: ", er.__class__)
                print('SQLite traceback: ')
                exc_type, exc_value, exc_tb = sys.exc_info()
                print(traceback.format_exception(exc_type, exc_value, exc_tb))

        return result



    def closeDB(self):
        if self.con:
            self.con.close()



if __name__ == "__main__":
    x = cls_dbAktionen()