import os
from influxdb import InfluxDBClient
from sqlalchemy import create_engine, text

class DB_Connect:
    '''
    A Class which will return PostgreSQL instance & Influx instance
    '''

    def __init__(self, **kwargs):
        self.PUSER = 'postgres'
        self.PPASSWORD = '212a4621b1fd7fc3'
        self.PPORT = '5432'
        self.PDATABASE = 'cbort_sandbox'
        self.PHOST = '10.139.219.13'
        self.IUSER = 'admin'
        self.IPASSWORD = 'admin'
        self.IPORT = '8086'
        self.IDATABASE = 'GreyOrange'
        self.IHOST = '172.28.62.168'
        self.__dict__.update(kwargs)

    def openconnection_postgres(self):
        try:
            pconnect = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(self.PUSER, self.PPASSWORD, self.PHOST, \
            self.PPORT, self.PDATABASE))
        except Exception as error:
            pconnect = "Failed to connect with Postgres database: {}".format(error)
        return pconnect
    
    def openconnection_influx(self):
        try:
            iconnect = InfluxDBClient(host=self.IHOST, port=self.IPORT, username=self.IUSER, password=self.IPASSWORD, database=self.IDATABASE)
            iconnect.switch_database(self.IDATABASE)
        except Exception as error:
            iconnect = "Failed to connect with Influx DB: {}".format(error)
        return iconnect
