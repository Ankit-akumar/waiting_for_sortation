import os
from influxdb import InfluxDBClient
from sqlalchemy import create_engine, text

class DB_Connect:
    '''
    A Class which will return PostgreSQL instance & Influx instance
    '''

    def __init__(self, **kwargs):
        self.PUSER = os.getenv('RMS_POSTGRES_USERNAME')
        self.PPASSWORD = os.getenv('RMS_POSTGRES_PASSWORD')
        self.PPORT = os.getenv('RMS_POSTGRES_PORT')
        self.PDATABASE = os.getenv('RMS_POSTGRES_DATABASE_CBORT')
        self.PHOST = os.getenv('RMS_POSTGRES_HOST')
        self.IUSER = os.getenv('RTP_INFLUX_USERNAME')
        self.IPASSWORD = os.getenv('RTP_INFLUX_PASSWORD')
        self.IPORT = os.getenv('RTP_INFLUX_PORT')
        self.IDATABASE = os.getenv('RTP_INFLUX_DATABASE')
        self.IHOST = os.getenv('RTP_INFLUX_HOST')
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
