"""
FASTAPI API to request a sensor
using the class VisitorCounter
and its method get_visitor_count
at a certain date
e.g. :
/get_visitor_count?date_time=2025-05-29_09:05
"""

from datetime import datetime

from fastapi import FastAPI, HTTPException
from src.API.data_app import create_agencies

db_path = 'src/API/data_app/db/AgencyDetails.duckdb'
table_name = 'AgencyDetails'

agencies_dict = create_agencies(db_path, table_name)     # read everything from the table

app = FastAPI()


@app.get("/get_visitor_count")
async def get_visitor_count(date_time: str,
                            agency_name:str,
                            counter_id:int=-1,
                            count_unit:str='visitors'):
    """
    This road gives back the visitor count for
    - a certain date_time
    - a certain agency_name
    - a certain counter_id (if omitted, then all traffic given)
    - with a unit to count the visitors
    :param date_time: string with format 'YYYY-MM-DD_HH:MM'
    :param agency_name: string with format 'AgencyName'
    :param counter_id: int
    :param count_unit: str
    :return: visitor count at this moment
    """
    try:
        date_time_obj = datetime.strptime(date_time, "%Y-%m-%d_%H:%M")

        agency = agencies_dict[agency_name]
        if counter_id > -1:
            count = agency.get_counter_traffic(date_time_obj,counter_id)
        else :
            count = agency.get_all_counter_traffic(date_time_obj)

        if count == -10:

            raise HTTPException(
                status_code=404,
                detail=f"No visitor counted on {date_time_obj.strftime("%A")} {date_time} "
                f"the sensor was broken.",
            )

        if count == -1 and date_time_obj.weekday() >= 5:
            raise HTTPException(
                status_code=404,
                detail=f"The bank was closed on {date_time_obj.strftime("%A")} {date_time} "
                f"(opened monday to friday).",
            )

        if count == -1 and date_time_obj.weekday() < 5:
            raise HTTPException(
                status_code=404,
                detail=f"The bank was closed on {date_time_obj.strftime("%A")} {date_time} "
                f"(aperture hours 9-12 13-18) (12:00 and 18:00 closed).",
            )

        if counter_id == -1 :   # all traffic
            return {"AgencyName": agency_name, "date_time": date_time, "visitor_count": count, 'unit': count_unit}
        else:
            return {"AgencyName": agency_name, "date_time": date_time, "visitor_count": count, 'unit': count_unit, 'counter_id': counter_id}

    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"""Error in the date {date_time}, Date should look like :2023-12-31_09:45 """,
        ) from exc

    except IndexError as exc:
        raise HTTPException(
            status_code=404,
            detail=f"""Error in the counter_id field, for agency {agency_name}, max counter_id = {agency.counter_num -1} """,
        ) from exc
