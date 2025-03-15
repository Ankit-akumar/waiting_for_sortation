from influxdb import InfluxDBClient
from sqlalchemy import create_engine, text

#DATABASE_URL = "postgresql://postgres:212a4621b1fd7fc3@10.139.219.13:5432/cbort_sandbox"

#engine = create_engine(DATABASE_URL)

#with engine.connect() as connection:
#    result = connection.execute(text("SELECT awb FROM data_shipment LIMIT 1;"))
#    print(result.fetchone())


host = "172.28.62.168"
port = 8086
database = "GreyOrange"

client = InfluxDBClient(host=host, port=port)
client.switch_database(database)

query = 'SELECT * FROM \"GreyOrange\".\"autogen\".\"zw_bot_events\" WHERE time > now() - 1d AND time < now() and processing_status = \'unprocessable\' order by time limit 1'
result = client.query(query)

for point in result.get_points():
    print(point)

client.close()


# class DB_Connect:
#     '''
#     A Class which will return PostgreSQL instance & Influx instance
#     '''

#     def __init__(self, **kwargs):
#         self.PUSER = ""
#         self.PPASSWORD = ""
#         self.PPORT = ""
#         self.PDATABASE = ""
#         self.PHOST = ""
#         # self.IUSER = os.getenv('IUSER')
#         # self.IPASSWORD = os.getenv('IPASSWORD')
#         # self.IPORT = os.getenv('IPORT')
#         # self.IDATABASE = os.getenv('IDATABASE')
#         # self.IHOST = os.getenv('IHOST')
#         # self.PHOST = os.getenv('PHOST')
#         # self.__dict__.update(kwargs)

#     def openconnection_postgres(self):
#         try:
#             pconnect = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(self.PUSER, self.PPASSWORD, self.PHOST, \
#             self.PPORT, self.PDATABASE))
#         except Exception as error:
#             pconnect = "Failed to connect with Postgres database: {}".format(error)
        
#         return pconnect

#     def openconnection_influx(self):
#         try:
#             iconnect = InfluxDBClient(host=self.IHOST, port=self.IPORT, username=self.IUSER, \
#             password=self.IPASSWORD, database=self.IDATABASE)
#         except Exception as error:
#             iconnect = "Failed to connect with Influx DB: {}".format(error)
        
#         return iconnect