from database import DB_Connect
from datetime import datetime
from utils import *
import os
from dotenv import load_dotenv

# loading env variables
load_dotenv()

# setting up logging
file_logger = setup_logger('debug')

file_logger.debug("Starting script execution...")

pengine_cbort = DB_Connect(**{
    "PUSER":os.getenv('RMS_POSTGRES_USERNAME'),
    "PPASSWORD":os.getenv('RMS_POSTGRES_PASSWORD'),
    "PHOST":os.getenv('RMS_POSTGRES_HOST'),
    "PPORT":os.getenv('RMS_POSTGRES_PORT'),
    "PDATABASE":os.getenv('RMS_POSTGRES_DATABASE_CBORT')
}).openconnection_postgres()

pengine_wms_process = DB_Connect(**{
    "PUSER":os.getenv('RTP_POSTGRES_USERNAME'),
    "PPASSWORD":os.getenv('RTP_POSTGRES_PASSWORD'),
    "PHOST":os.getenv('RTP_POSTGRES_HOST'),
    "PPORT":os.getenv('RTP_POSTGRES_PORT'),
    "PDATABASE":os.getenv('RTP_POSTGRES_DATABASE')
}).openconnection_postgres()

iengine_greyorange = DB_Connect(**{
    "PUSER":os.getenv('RTP_INFLUX_USERNAME'),
    "PPASSWORD":os.getenv('RTP_INFLUX_PASSWORD'),
    "PHOST":os.getenv('RTP_INFLUX_HOST'),
    "PPORT":os.getenv('RTP_INFLUX_PORT'),
    "PDATABASE":os.getenv('RTP_INFLUX_DATABASE')
}).openconnection_influx()

execution_method = input("Please enter the execution method: 0 for bot id & 1 for pps_id & 2 for getting all error bots in past 6 hours : ")

if execution_method == '1':

    # CASE 1: When pps ID and timestamp is given

    timestamp=input("Please enter the timestamp in UTC format(yyyy-mm-dd HH:MM): ")
    front_pps_id = input("Please enter the PPS ID: ")

    # Formating timestamp

    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
    timestamp_start = dt.replace(second=0).isoformat() + "Z"
    timestamp_end = dt.replace(second=59).isoformat() + "Z"

    file_logger.debug(f"formatted timestamp influxDB - {timestamp}, timestamp_start - {timestamp_start}, timestamp_end - {timestamp_end}")

    failed_arrived_msg = get_failed_arrived_msg_by_ppsID(iengine_greyorange=iengine_greyorange, timestamp_start=timestamp_start, timestamp_end=timestamp_end, front_pps_id=front_pps_id)

    points_list = list(failed_arrived_msg.get_points())  # Convert iterator to list

    if points_list:  
        execute_by_bot(pengine_cbort=pengine_cbort, points_list=points_list, file_logger=file_logger)
    else:
        print()
        file_logger.error(f"No failed arrival messages found in the past 3 days")
        print(f"No failed arrival messages found in the past 3 days")


elif execution_method == '0':
    # CASE 2: When bot ID is given

    bot_id = input("Please enter the bot id: ")

    # Getting failed arrived message
    failed_arrived_msg = get_failed_arrived_msg_by_botID(bot_id=bot_id, iengine_greyorange=iengine_greyorange)

    points_list = list(failed_arrived_msg.get_points())  # Convert iterator to list

    if points_list: 
        execute_by_bot(pengine_cbort=pengine_cbort, points_list=points_list, file_logger=file_logger)
    else:
        print()
        file_logger.error(f"No failed arrival messages found in the past 3 days")
        print(f"No failed arrival messages found in the past 3 days")


else:
    # CASE3: Getting all failed bots in the past 6 hours

    df = get_all_failed_arrived_msg(iengine_greyorange=iengine_greyorange, file_logger=file_logger)

    failed_bots = get_failed_bots(pengine_cbort=pengine_cbort, df=df, file_logger=file_logger)

    if len(failed_bots) != 0:
        print("Now testing for each failed bot...")
        for bot_id in failed_bots:
            failed_arrived_msg = get_failed_arrived_msg_by_botID(bot_id=bot_id, iengine_greyorange=iengine_greyorange)

            points_list = list(failed_arrived_msg.get_points())  # Convert iterator to list

            if points_list: 
                execute_by_bot(pengine_cbort=pengine_cbort, points_list=points_list, file_logger=file_logger)
            else:
                print()
                file_logger.error(f"No failed arrival messages found in the past 3 days")
                print(f"No failed arrival messages found in the past 3 days")



file_logger.debug("Script executed")

