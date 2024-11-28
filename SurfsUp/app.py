# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB

#################################################
# Functions
#################################################
def most_active_station():
    session = Session(engine)
    # Query Measurement for the most active station
    most_active_station = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()
    # Isolate the string containing the station ID
    most_active_station_id = most_active_station[0]
    session.close()
    return most_active_station_id

def tobs_query(station, start_date, end_date):
    session = Session(engine)
    # Query Measurement for one station from start_date to end_date
    results = session.query(Measurement.date,
        Measurement.tobs).filter(Measurement.date >= start_date,
                                 Measurement.date <= end_date,
                                 Measurement.station == station).order_by(Measurement.date).all()
    session.close()
    return results

def temp_min_max_avg(station, start_date, end_date):
    session = Session(engine)
    # Query
    temp_results = session.query(
        func.min(Measurement.tobs).label('lowest'),
        func.max(Measurement.tobs).label('highest'),
        func.avg(Measurement.tobs).label('average')).filter(Measurement.station == station,
                                                            Measurement.date >= start_date,
                                                            Measurement.date <= end_date).all()
    session.close()
    return jsonify({
        station: [
            start_date,
            end_date: [
                
            ]
        ]
    })
    
#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route('/')
def home():
    '''List all available api routes.'''
    return jsonify({
        'Available Routes': [
            '/api/v1.0/precipitation',
            '/api/v1.0/stations',
            '/api/v1.0/tobs',
            '/api/v1.0/<start>',
            '/api/v1.0/<start>/<end>'
        ]
    })

@app.route('/api/v1.0/precipitation')
def precipitation():
    session = Session(engine)
    '''Return all precipitation data'''
    recent_date = '2017-08-23'
    prev_year = '2016-08-23'
    results = session.query(Measurement.date,
                            Measurement.prcp).\
        filter(Measurement.date >= prev_year,
            Measurement.date <= recent_date).\
        order_by(Measurement.date).all()
    session.close()
    
    last_year = []
    for date, prcp in results:
        last_year_dict = {date: prcp}
        
        last_year.append(last_year_dict)
    return jsonify(last_year)        
        
    
@app.route('/api/v1.0/stations')
def stations():
    session = Session(engine)
    '''Return a list of all stations'''
    results = session.query(Station.station,
                            Station.name,
                            Station.latitude,
                            Station.longitude,
                            Station.elevation).all()
    session.close()
    
    all_stations = []
    for id, name, latitude, longitude, elevation in results:
        station_dict = {
            id: {
                'name': name, 
                'latitude': latitude, 
                'longitude': longitude, 
                'elevation': elevation
            }
        }
        
        all_stations.append(station_dict)
    
    return jsonify(all_stations)


@app.route('/api/v1.0/tobs')
def tobs():
    '''Return a list of dates and temperature observations from the most active station for the previous year'''
    session = Session(engine)
    station = most_active_station()
    
    end_date = '2017-08-23'
    start_date = '2016-08-23'
    
    results = tobs_query(station, start_date, end_date)
   
    last_year = []
    for date, tobs in results:
        last_year_dict = {date: tobs}
        last_year.append(last_year_dict)
        
    output = {
        station: last_year}

    return jsonify(output)  

@app.route('/api/v1.0/<start>')
def start():
    session = Session(engine)
    '''Return a list of dates and temperature observations from the most active station for the previous year'''
    most_active_station = session.query(Measurement.station).\
    group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()
    most_active_station_id = most_active_station[0]
    
    recent_date = '2017-08-23'
    prev_year = '2016-08-23'
    
    results = session.query(Measurement.date,
                        Measurement.tobs).filter(Measurement.date >= prev_year,
            Measurement.date <= recent_date,
            Measurement.station == most_active_station_id).order_by(Measurement.date).all()
    session.close()
   
    last_year = []
    for date, tobs in results:
        last_year_dict = {date: tobs}
        
        last_year.append(last_year_dict)
    output = {
        most_active_station_id: last_year}
    # return jsonify(last_year)
    return jsonify(output)  

if __name__ == '__main__':
    app.run(debug=True)