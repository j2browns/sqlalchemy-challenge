######################################################33
#SQLAlchemy Challenge Homework 
#Jeff Brown

###Flask portion of assignment

#Importing required functions
import numpy as np
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,  inspect, distinct

#Setting up connection to SQL-Lite and Base Connections
#This work is based on testing in Jupyter Notebook from Same Assignment
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
#creating object for measurement and station
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(engine)

############################################################
#Getting information for station list
#Taken directly from Query in Jupyter Notebook
Stations = session.query(Station.station, Station.name).all() 
session.close()

station_list = []
station_name = []
for record in Stations:
    station = record[0]
    name = record[1]
    station_list.append(station)
    station_name.append(name)

station_counts_df = pd.DataFrame(columns = ["Station","Counts"])
for stn in station_list:
    temp = session.query(Measurement.station,func.count(Measurement.station)).\
                filter(Measurement.station == stn).\
                order_by(func.count(Measurement.station)).all()
    station_counts_df = station_counts_df.append({"Station":temp[0][0],"Counts":temp[0][1]}, ignore_index = True)
session.close()
station_counts_df.sort_values(["Counts"],ascending = False,inplace=True)
station_info_dict = dict(zip(station_list,station_name))
station_info_dict
###################################################################

###################################################################
#precipitation information
#Analysis of precipitation data to find the maximum date in the sheet.
#Taken from Jupyter Notebook.  Added conversion to dictionary
sel = [ Measurement.date, Measurement.prcp] #pulling only date and precipitation.
weather_data = session.query(*sel).all() 
session.close()

min_date_record = dt.date(3000,1,1)
max_date = dt.date(1900,1,1) #need this as dummy for starting to analyze the data.  If this was returned as max date
                                #would have to set as lower number.
for record in weather_data:
    (ME_date, ME_Prec) = record
    if ME_date > str(max_date):
        max_date = ME_date 
    if ME_date < str(min_date_record):
        min_date_record = ME_date 

YMD = max_date.split('-')
max_date = dt.date(int(YMD[0]),int(YMD[1]),int(YMD[2]))
min_date = max_date - dt.timedelta(days=365)

weather_filter = session.query(*sel).\
    filter((Measurement.date <= max_date)& (Measurement.date >= min_date)).all()
session.close()

#creating dataframe with information as dictated in directions.
precip_df = pd.DataFrame(columns = ["Date","Precip"])
for record in weather_filter:
    (ME_date, ME_Prec) = record
    precip_df = precip_df.append({"Date":ME_date, "Precip":ME_Prec}, ignore_index = True)

precip_df.dropna(inplace = True)
precip_df = precip_df.set_index("Date")
precip_df.sort_index(inplace=True)
#dataframe includes data from many stations. Therefore perform groupby on date and then average
# for day of all stations.
precip_groupby = precip_df.groupby("Date")
precip_group_series = precip_groupby["Precip"].mean()
precip_group_df = pd.DataFrame({'Date':precip_group_series.index, 'Precip':precip_group_series.values})
precip_group_df = precip_group_df.set_index('Date')
precip_group_df.sort_index(inplace=True)
precip_group_df

####output Dictionnary - to be used in JSON
precip_dictionary = precip_group_df.to_dict('dict')
####################################################################

####################################################################
##Temperature Data for most active station
#Taken from Jupyter Notebook
Stations = session.query(Station.station, Station.name).all() 
session.close()

station_list = []
station_name = []
for record in Stations:
    station = record[0]
    name = record[1]
    station_list.append(station)
    station_name.append(name)
print("")

station_counts_df = pd.DataFrame(columns = ["Station","Counts"])
for stn in station_list:
    temp = session.query(Measurement.station,func.count(Measurement.station)).\
                filter(Measurement.station == stn).\
                order_by(func.count(Measurement.station)).all()
    station_counts_df = station_counts_df.append({"Station":temp[0][0],"Counts":temp[0][1]}, ignore_index = True)
session.close()
station_counts_df.sort_values(["Counts"],ascending = False,inplace=True)
station_max = station_counts_df.iloc[0][0]
# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
sel = [Measurement.station,Measurement.date, Measurement.tobs]
temp_survey = session.query(*sel).\
                filter(Measurement.station == station_max).\
                filter((Measurement.date <= max_date)& (Measurement.date >= min_date)).all()

temp_survey_df = pd.DataFrame(columns = ["Station","Date", "Temp"])
for record in temp_survey:
    (Me_date, Me_stn,Me_temp) = record
    temp_survey_df = temp_survey_df.append({"Station":Me_date,"Date":Me_stn,"Temp":Me_temp}, ignore_index = True)

####output Dictionnary - to be used in JSON
temp_survey_dict = temp_survey_df.to_dict('index')


################################################
################################################
from flask import Flask, jsonify
#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
#################################################
#SQLAlchemy has a thread error. Must remake connection to database and create engine.
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
#checking names of tables contained in sql database
Base = automap_base()
Base.prepare(engine, reflect=True)
#creating object for measurement and station
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(engine)
##################################################
@app.route("/")
def welcome():
    return (
        f"Welscom to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"<br/>"
        f"To query the min, max and average temp starting from date to last observation use below<br/>"
        f"Where YYYY is four digit year, MM is 2 digit month, DD is 2 digit day<br/>"
        f"Earliest date is {min_date_record}<br/>"
        f"Latest date is {max_date}<br/>"
        f"<br/>"
        f"/api/v1.0/YYYY-MM-DD<br/>"
        f"<br/>"
        f"To query the min, max and average temp starting from date to another date use below<br/>"
        f"Where YYYY is four digit year, MM is 2 digit month, DD is 2 digit day<br/>"
        f"First date is start, second is ending<br/>"
        f"<br/>"
        f"/api/v1.0/YYYY-MM-DD/YYYY-MM-DD<br/>"
            )
#################################################
@app.route("/api/v1.0/stations")
def stations():
    """Return information about weather stations"""
    
    return jsonify(station_info_dict)
#################################################
@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return information about precipitation over 1 year period"""
    
    return jsonify(precip_dictionary)
#################################################

@app.route("/api/v1.0/tobs")
def tobs():
    """Return information about temperature in most active site over 1 year period"""
    
    #solution provided above before flask code
    return jsonify(temp_survey_dict)


@app.route("/api/v1.0/<date>")
def date_to_end(date):
    """Return information about temperature in most active site over 1 year period"""
    
    sel = [ Measurement.date, Measurement.prcp] #pulling only date and precipitation.
    weather_data = session.query(*sel).all() 
    session.close()

    min_date_record = dt.date(3000,1,1)
    max_date = dt.date(1900,1,1) #need this as dummy for starting to analyze the data.  If this was returned as max date
                                    #would have to set as lower number.
    for record in weather_data:
        (ME_date, ME_Prec) = record
        if ME_date > str(max_date):
            max_date = ME_date
        if ME_date < str(min_date_record):
            min_date_record = ME_date

 #includes some basic error handling for date range   
    if (date >= min_date_record) and (date <= max_date):
        sel = [ func.min(Measurement.tobs), func.avg(Measurement.tobs),func.max(Measurement.tobs)] 
        temp_detail, = session.query(*sel).\
            filter((Measurement.date <= max_date)& (Measurement.date >= date)).all()
        session.close()
        temp_to_end_dict = {"Start_Date":date, "End_Date":max_date, "Min_Temp":temp_detail[0],
                            "Avg_Temp":temp_detail[1],"Max_Temp":temp_detail[2]}
        return jsonify(temp_to_end_dict)
    else:
        return jsonify({"test": f"Entered date {date} is not in range min: {min_date_record} and max:{max_date}"}), 404
#################################################

@app.route("/api/v1.0/<date1>/<date2>")
def date_range(date1,date2):
    """Return information about temperature in most active site over 1 year period"""
      
    sel = [ Measurement.date, Measurement.prcp] #pulling only date and precipitation.
    weather_data = session.query(*sel).all() 
    session.close()

    min_date_record = dt.date(3000,1,1)
    max_date = dt.date(1900,1,1) #need this as dummy for starting to analyze the data.  If this was returned as max date
                                    #would have to set as lower number.
    for record in weather_data:
        (ME_date, ME_Prec) = record
        if ME_date > str(max_date):
            max_date = ME_date
        if ME_date < str(min_date_record):
            min_date_record = ME_date

#includes some basic error handling for date range
    if (date1 >= min_date_record) and (date2 <= max_date) and (date2 > min_date_record) and (date1 < max_date):
        sel = [ func.min(Measurement.tobs), func.avg(Measurement.tobs),func.max(Measurement.tobs)] 
        temp_detail, = session.query(*sel).\
            filter((Measurement.date <= date2)& (Measurement.date >= date1)).all()
        session.close()
        temp_range = {"Start_Date":date1, "End_Date":date2, "Min_Temp":round(temp_detail[0],1),
                            "Avg_Temp":round(temp_detail[1],1),"Max_Temp":round(temp_detail[2],1)}
        return jsonify(temp_range)
    else:
        return jsonify({"test": f"Entered dates min:{date1} and max:{date2} is not in range min: {min_date_record} and max:{max_date}"}), 404
    

if __name__ == "__main__":
    app.run(debug=True)