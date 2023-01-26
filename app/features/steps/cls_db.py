import mysql.connector
import sys
import traceback
from configparser import ConfigParser
import os
import pathlib


class cls_dbAktionen():
    def __init__(self, zieldb=None):
        config_path = pathlib.Path(__file__).parent.absolute() / "../../config/config.ini"
        print("config:", config_path)
        config = ConfigParser()
        config.read(config_path)
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
        self.mydb = mysql.connector.connect(host=configuration['host'], user=configuration['user'], password=configuration['password'], database=configuration['database'])
    #    self.mydb = mysql.connector.connect(host="rzp-mysql", user="root", password="root", database="rzp_git")

    def execSql(self, statement, val):
        mycursor = self.mydb.cursor()
        try:
            if val:
                mycursor.execute(statement, val)
            else:
                mycursor.execute(statement)
            self.mydb.commit()
            lastRowId = mycursor.lastrowid

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