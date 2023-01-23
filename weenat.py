from sqlalchemy import create_engine, Column, Integer, String, select, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests, re
import pandas as pd
from dateutil import parser as pda
from flask import Flask, request, jsonify
from flask_restful import Resource, Api, reqparse, inputs
from datetime import datetime
import numpy as np
from isodate import parse_datetime

app = Flask(__name__)




""" 
Get data and insert into sqlite db
"""
engine = create_engine('sqlite:///measure.db')
Base = declarative_base()
class Measure(Base):
    __tablename__ = 'measure'
    id = Column(Integer, primary_key=True)
    timestamp = Column(String)
    hum = Column(String)
    temp = Column(String)
    precip = Column(String)

parser = reqparse.RequestParser()
parser.add_argument('since', type=str, required=False, location='args', help='Filter by date and time. Ingestion date of returned records should be higher than the value provided. Format expected ISO-8601.!')
parser.add_argument('before', type=str, required=False, default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), location='args', help='Filter by date and time. Ingestion date of returned records should be lower than the value provided. Default is now. Format expected ISO-8601.')
parser.add_argument('span', type=str, required=False, location='args', help='Aggregates data given this parameter. Default value should be raw (meaning no aggregate)')
parser.add_argument('datalogger', type=str, required=True, location='args', help='Filter by datalogger. This field is required. Should be an exact match of the datalogger id')


query_all = select([Measure])

def insert_data_once():

    Base.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    measures = session.query(Measure).all()

    if not measures:
        response = requests.get("http://localhost:3000/measurements")
        data = response.json()
        for item in data:
            for i,k in item.items() :
                measure = Measure( timestamp=i, hum=k["hum"], temp=k["temp"], precip=k["precip"])
                session.add(measure)
        session.commit()
    session.close()

insert_data_once()

def datetime_to_timestamp(date_string):
    date_object = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
    timestamp = int(date_object.timestamp())*1000
    return str(timestamp)


def checkparam(arg):
    try:
        if (arg['since'] is None or parse_datetime(arg['since'])) and (parse_datetime(arg['before'])) :
            return True
    except:
        return False

@app.route('/api/summary/', methods=['GET'])
def summary():
    args = parser.parse_args()
    if (checkparam(args) != True) :
            return ('Missing required values.', 400)
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        df = pd.read_sql_query(query_all, engine)
        df.index = pd.to_datetime(df.index)

        if args['since'] :
            # http://127.0.0.1:5000/api/summary/?datalogger=measurment&since=2021-01-01T02:59:40&before=2021-01-01T01:59:40&span=day
            df_filtered = df.loc[  (df['timestamp'] < datetime_to_timestamp(args['before'])) & (df['timestamp'] > datetime_to_timestamp(args['since']))]
        else:
            # http://127.0.0.1:5000/api/summary/?datalogger=measurment&before=2021-01-01T01:59:40&span=day
            df_filtered = df.loc[(df['timestamp']) < datetime_to_timestamp(args['before']) ]

        if args['span'] and (args['span'] == 'day' or args['span'] == 'hour' ):
            #http://127.0.0.1:5000/api/summary/?datalogger=measurment&since=2021-01-01T02:59:40&before=2021-01-01T03:59:40&span=day
            df_filtered[['temp', 'hum', 'precip']] = df_filtered[['temp', 'hum','precip']].apply(pd.to_numeric, errors='coerce')

            span_map = {'day': 'D', 'hour': 'H'}
            s = span_map.get(args['span'])
            aggregated_data = df_filtered.resample(s).agg({'temp': 'mean', 'hum': 'mean', 'precip': 'sum'})
            res = aggregated_data.to_numpy()
            data = np.ndarray.tolist(res)
            session.close()
            return jsonify(data)
        elif args['span'] == 'max':
            #http://127.0.0.1:5000/api/summary/?datalogger=measurment&since=2021-01-01T02:59:40&before=2021-01-01T03:59:40&span=max
            aggregated_data = df_filtered[['temp', 'hum', 'precip']].max()

            res = aggregated_data.to_numpy()
            data = np.ndarray.tolist(res)
            session.close()
            return jsonify(data)

        res = df_filtered.to_numpy()
        data = np.ndarray.tolist(res)
        session.close()
        return jsonify(data)
    except Exception as e:
        print(e)
        return ('Missing required values.', 400)

@app.route('/api/data/', methods=['GET'])
def data():
    args = parser.parse_args()
    if checkparam(args) != True:
        print(args['before'])
        return ('Missing required valuess.', 400)
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        df = pd.read_sql_query(query_all, engine)
        if args['since'] :
            # http://127.0.0.1:5000/api/data/?datalogger=measurment&since=2021-01-01T02:59:40&before=2021-01-01T01:59:40
            df_filtered = df.loc[  (df['timestamp'] < datetime_to_timestamp(args['before'])) & (df['timestamp'] > datetime_to_timestamp(args['since']))]
        else:
            # http://127.0.0.1:5000/api/data/?datalogger=measurment&before=2021-01-01T01:59:40
            df_filtered = df.loc[(df['timestamp']) < datetime_to_timestamp(args['before']) ]

        res = df_filtered.to_numpy()
        data = np.ndarray.tolist(res)
        session.close()

        return jsonify(data)
    except Exception as e:
        print(e)
        return ('Missing required values.', 400)

if __name__ == '__main__':
    app.run(debug=True)