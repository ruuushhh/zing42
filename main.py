from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import pandas as pd
import io, requests, zipfile
from datetime import date, timedelta
from config.db import conn

app = FastAPI()


@app.get("/get_securities_available_for_equity_csv")
async def root():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    df1 = pd.read_csv(url)    
    stream = io.StringIO()
    df1.to_csv('csv_files/Securities_available_for_equity.csv')
    df1.to_csv(stream, index = False)
    response = StreamingResponse(iter([stream.getvalue()]),
                        media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=Securities_available_for_equity.csv"
    return response

@app.get("/get_bhavcopy_csv")
async def root():
    x = date.today() - timedelta(days=1)
    url =  "https://archives.nseindia.com/content/historical/EQUITIES/2022/OCT/cm" + str((x.strftime("%d%b%Y").upper()))  +"bhav.csv.zip"
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))    
    z.extractall("csv_files")
    df2 = pd.read_csv("csv_files\cm25OCT2022bhav.csv")    
    stream = io.StringIO()
    df2.to_csv(stream, index = False)
    response = StreamingResponse(iter([stream.getvalue()]),
                        media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=bhav_copy.csv"
    return response


@app.get("/get_bhavcopy_csv_for_last_30_days")
async def root():
    for i in range(1, 30):
        x = date.today() - timedelta(days=i)
        if(x.strftime("%w") != "6"  and x.strftime("%w") != "0"):
            url =  "https://archives.nseindia.com/content/historical/EQUITIES/" + str(x.strftime("%Y")) +"/" + str((x.strftime("%b")).upper()) + "/cm" + str((x.strftime("%d%b%Y").upper()))  +"bhav.csv.zip"
            r = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(r.content))    
            z.extractall("bhavcopy_last_30_days")
    return {"folder_path": "bhavcopy_last_30_days"}


@app.post("/insert_equity_data_in_database")
async def root():
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    df1 = pd.read_csv(url)  
    data = df1.to_dict(orient="records")
    db = conn["nse"]
    db.Equity.insert_many(data)
    return {"message" : "Done"}


@app.post("/insert_bhavcopy_in_database")
async def root():
    x = date.today() - timedelta(days=1)
    url =  "https://archives.nseindia.com/content/historical/EQUITIES/2022/OCT/cm" + str((x.strftime("%d%b%Y").upper()))  +"bhav.csv.zip"
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))    
    z.extractall("csv_files")
    df2 = pd.read_csv("csv_files\cm25OCT2022bhav.csv") 
    data = df2.to_dict(orient="records")
    db = conn["nse"]
    db.BhavCopy.insert_many(data)
    return {"message" : "Done"}








