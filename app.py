from database import DB_Connect
from datetime import datetime
from utils import *
import os
from dotenv import load_dotenv

load_dotenv()

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

execution_method = input("Please enter the execution method: 0 for bot id & 1 for pps_id: ")

if execution_method == '1':

    # CASE 1: When pps ID and timestamp is given

    # iengine_sms = DB_Connect().openconnection_influx_sms()

    timestamp=input("Please enter the timestamp in UTC format(yyyy-mm-dd HH:MM): ")
    front_pps_id = input("Please enter the PPS ID: ")

    # Formating timestamp

    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
    timestamp_start = dt.replace(second=0).isoformat() + "Z"
    timestamp_end = dt.replace(second=59).isoformat() + "Z"
    # print(timestamp)
    # print(timestamp_start)
    # print(timestamp_end)

    failed_arrived_msg = get_failed_arrived_msg_by_ppsID(iengine_greyorange=iengine_greyorange, timestamp_start=timestamp_start, timestamp_end=timestamp_end, front_pps_id=front_pps_id)

    points_list = list(failed_arrived_msg.get_points())  # Convert iterator to list

else:
    # CASE 2: When bot ID is given

    bot_id = input("Please enter the bot id: ")

    # Getting failed arrived message
    failed_arrived_msg = get_failed_arrived_msg_by_botID(bot_id=bot_id, iengine_greyorange=iengine_greyorange)

    points_list = list(failed_arrived_msg.get_points())  # Convert iterator to list


# Getting shipments the bot was carrying before arrived event failed

if points_list:  # Check if there is any data
    first_point = points_list[0]  # First item

    time, back_pps_id, front_pps_id, bin_id, processing_failure_reason = None, None, None, None, None

    for point in points_list:  # Iterate over all points
        time = point["received_at"]
        back_pps_id = point["back_pps_id"]
        front_pps_id = point["front_pps_id"]
        bin_id = point["bin_id"]
        bot_id = point["bot_id"]
        processing_failure_reason = point["processing_failure_reason"]

    print(f"Bot ID {bot_id} sent an unprocessable arrived event at {time} from back PPS {back_pps_id}, front PPS {front_pps_id} and bin {bin_id} with failure reason - {processing_failure_reason}")

    # formatting time for postgres query
    dt = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    # print(formatted_time)

    # Getting previous shipments sorted by the bot
    shipments = get_shipments_from_bot(time=formatted_time,pengine_cbort=pengine_cbort,bot_id=bot_id)

    # printing shipments
    if shipments:
        print_shipments(shipments=shipments)
        link_to_sop='https://work.greyorange.com/confluence/x/aWKxDQ'
        print(f"Kindly send the drop events manually. Here is the link to SOP - {link_to_sop}")
    else:
        print(f"ERROR: No shipments found for the given bot {bot_id} before {time}")

else:
    print(f"No failed arrival messages found in the past 3 days")


    # # Getting induct ID from induct_pps_mapping
    # induct_id = get_induct_id(front_pps_id=front_pps_id, pengine_wms_process=pengine_wms_process)
    # print(f"Induct ID is {induct_id}")

    # if induct_id:
    #         # Getting bot ids that arrived on the induct during the time interval
    #         arrived_bots = get_arrived_bots(iengine_sms=iengine_sms, timestamp_start=timestamp_start, timestamp_end=timestamp_end, induct_id=induct_id)

    #         points_itr = arrived_bots.get_points()
    #         first_point = next(points_itr,None)

    #         if first_point:
    #             for point in arrived_bots.get_points():
    #                 print(point['bot_id'])

    # else:
    #     print(f"ERROR: Could not find induct id from induct_pps_mapping for front_pps_id = \'{front_pps_id}\'")