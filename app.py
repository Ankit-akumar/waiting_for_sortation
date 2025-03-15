from database import DB_Connect
from sqlalchemy import text
from datetime import datetime
from utils import *

pengine_cbort = DB_Connect().openconnection_postgres_cbort()
iengine_autogen = DB_Connect().openconnection_influx_autogen()
pengine_wms_process = DB_Connect().openconnection_postgres_wms_process()

execution_method = input("Please enter the execution method: -b for bot id & -p for front_pps_id")
if execution_method == '-b':
    
    # CASE 1: When bot ID is given

    bot_id = input("Please enter the bot id: ")

    # Getting failed arrived message
    failed_arrived_msg = get_failed_arrived_msg(bot_id=bot_id, iengine_autogen=iengine_autogen)

    points_itr = failed_arrived_msg.get_points()
    first_point = next(points_itr,None)

    if first_point:
    # if a failed msg was found then retrieving the necessary details

        time, back_pps_id, front_pps_id, processing_failure_reason = None, None, None, None

        for point in failed_arrived_msg.get_points():
           time = point["received_at"]
           back_pps_id = point["back_pps_id"]
           front_pps_id = point["front_pps_id"]
           processing_failure_reason = point["processing_failure_reason"]

        print(f"Bot ID {bot_id} sent an unprocessable arrived event at {time} from back PPS {back_pps_id} and front PPS {front_pps_id} with failure reason - {processing_failure_reason}")

        # Getting shipments the bot was carrying before arrived event failed    
        
        # formatting time for postgres query
        dt = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
        formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
        print(formatted_time)

        # Getting previous shipments sorted by the bot
        shipments = get_shipments_from_bot(time=formatted_time,pengine_cbort=pengine_cbort,bot_id=bot_id)

        # printing shipments 
        if shipments:
            print_shipments(shipments=shipments)
        else:
            print("ERROR: No shipments found for the given bot_id.")

    else:
        print(f"No failed arrival messages found for bot {bot_id} in the past 3 days")


else:
    # Case 2: When induct ID and timestamp is given

    iengine_sms = DB_Connect().openconnection_influx_sms()

    timestamp=input("Please enter the timestamp in UTC format(yyyy-mm-dd HH:MM): ")
    front_pps_id = input("Please enter the Front PPS ID: ")

    # Formating timestamp

    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
    timestamp_start = dt.replace(second=0).isoformat() + "Z"
    timestamp_end = dt.replace(second=59).isoformat() + "Z"
    print(timestamp)
    print(timestamp_start)
    print(timestamp_end)

    # Getting induct ID from induct_pps_mapping
    induct_id = get_induct_id(front_pps_id=front_pps_id, pengine_wms_process=pengine_wms_process)
    print(f"Induct ID is {induct_id}")

    if induct_id:
            # Getting bot ids that arrived on the induct during the time interval
            arrived_bots = get_arrived_bots(iengine_sms=iengine_sms, timestamp_start=timestamp_start, timestamp_end=timestamp_end, induct_id=induct_id)

            points_itr = arrived_bots.get_points()
            first_point = next(points_itr,None)

            if first_point:
                for point in arrived_bots.get_points():
                    print(point['bot_id'])

    else:
        print(f"ERROR: Could not find induct id from induct_pps_mapping for front_pps_id = \'{front_pps_id}\'")