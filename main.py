from datetime import datetime

from fastapi import FastAPI, HTTPException
from src.sensor import VisitorCounter

app = FastAPI()

@app.get("/get_visitor_count")
async def get_visitor_count(date_time: str):
    """
    this road t
    :param date_time: string with format 'YYYY-MM-DD_HH:MM'
    :return: visitor count at this moment
    """
    counter = VisitorCounter()

    try :
        date_time_obj = datetime.strptime(date_time, "%Y-%m-%d_%H:%M")

        count = counter.get_visit_count(date_time_obj)

        if count == -10:
            raise HTTPException(status_code=404, detail=f"No visitor counted on {date_time_obj.strftime("%A")} {date_time} the sensor was broken.")

        if count == -1 and date_time_obj.weekday() >= 5:
            raise HTTPException(status_code=404, detail=f"The bank was closed on {date_time_obj.strftime("%A")} {date_time} (opened monday to friday).")

        if count == -1 and date_time_obj.weekday() < 5:
            raise HTTPException(status_code=404, detail=f"The bank was closed on {date_time_obj.strftime("%A")} {date_time} (aperture hours 9-12 13-18) (12:00 and 18:00 closed).")

        return {"date_time": date_time, "visitor_count": count}

    except ValueError:
        raise HTTPException(status_code=404,
                            detail=f'''Error in the date {date_time} Date should look like :2023-12-31_09:45 ''')

