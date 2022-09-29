from time import sleep
import pandas_datareader.data as web
import asyncio
from datetime import datetime
import schedule
import time
import threading
import csv
import sqlite3


def get_financial_data(today, ticket): 
    return web.get_data_yahoo(ticket, start=today, end=today)

def insert_cota(High, Low, Open, Close, Volume, AdjClose, dt):
    con = sqlite3.connect("dbteste.db")

    cur = con.cursor()

    cur.execute(""" 
    CREATE TABLE IF NOT EXISTS Cotas (
        id	INTEGER PRIMARY KEY AUTOINCREMENT,
        High	REAL,
        Low	REAL,
        Open	REAL,
        Close	REAL,
        Volume	REAL,
        AdjClose	REAL,
        InsertTime	TEXT
    )
    """)
    queryInsert = """
INSERT INTO 
    Cotas  (High, Low, Open, Close, Volume, AdjClose, InsertTime)
VALUES """ + f"({High},{Low},{Open},{Close},{Volume},{AdjClose},'{dt}')"
    print(queryInsert)
    cur.execute(queryInsert)
    con.commit()
    con.close()

def __main__():
    today = datetime.today().strftime('%Y-%m-%d')
    ticket = 'BOVA11.SA'
    print("Buscando...")
    data = get_financial_data(today, ticket)

    lista = data.values.tolist()[0]

    High = lista[0]
    Low = lista[1]
    Open = lista[2]
    Close = lista[3]
    Volume = lista[4]
    AdjClose = lista[5]
    print([High, Low, Open, Close, Volume, AdjClose, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    insert_cota(High, Low, Open, Close, Volume, AdjClose, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

print('INICIOU!')
schedule.every().minute.at(":00").do(__main__)

while True:
    schedule.run_pending()
    time.sleep(1)