'''
01_flight_etl.py

Purpose: Take data from OpenSky dataset clean, reduce, and join then save to parquet for later use

Inputs: data directory, airport lookup csv, airline lookup csv, output directory
Outputs: parquet files continaing
+--------+----+------------+-------------+-------------+-----------------+------+-------------------+----------------+--------------+-----------+-------------------------+---------------------+-------------------+---+
|callsign|icao|airline_name|aircraft_type|airliner_type|aircraft_category|origin|origin_airport_type|origin_continent|origin_country|destination|desitination_airport_type|destination_continent|destination_country|day|
+--------+----+------------+-------------+-------------+-----------------+------+-------------------+----------------+--------------+-----------+-------------------------+---------------------+-------------------+---+

Example call: spark-submit 01_flight_etl.py input_data airports.csv airlines.csv output

Original Author: Rollin Poe
Last Modified: 2020-07-28
Reviewed:
Verified:
'''

import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('Flight Data ETL').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

flight_schema = types.StructType([
    types.StructField('callsign', types.StringType()),
    types.StructField('number', types.StringType()),
    types.StructField('icao24', types.StringType()),
    types.StructField('registration', types.StringType()),
    types.StructField('typecode', types.StringType()),
    types.StructField('origin', types.StringType()),
    types.StructField('destination', types.StringType()),
    types.StructField('firstseen', types.DateType()),
    types.StructField('lastseen', types.DateType()),
    types.StructField('day', types.DateType()),
    types.StructField('latitude_1', types.FloatType()),
    types.StructField('longitude_1', types.FloatType()),
    types.StructField('altitude_1', types.FloatType()),    
    types.StructField('latitude_2', types.FloatType()),
    types.StructField('longitude_2', types.FloatType()),
    types.StructField('altitude_2', types.FloatType()),
])

airport_schema = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('ident', types.StringType()),
    types.StructField('type', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('latitude_deg', types.LongType()),
    types.StructField('longitude_deg', types.LongType()),
    types.StructField('elevation_ft', types.IntegerType()),
    types.StructField('continent', types.StringType()),
    types.StructField('iso_country', types.StringType()),
    types.StructField('iso_region', types.StringType()),
    types.StructField('municipality', types.StringType()),
    types.StructField('scheduled_service', types.StringType()),
    types.StructField('gps_code', types.StringType()),    
    types.StructField('iata_code', types.StringType()),
    types.StructField('local_code', types.StringType()),
    types.StructField('home_link', types.StringType()),
    types.StructField('wikipedia_link', types.StringType()),
    types.StructField('keywords', types.StringType()),
])

airline_schema = types.StructType([
    types.StructField('of_airline_id', types.IntegerType()),
    types.StructField('name', types.StringType()),
    types.StructField('alias', types.StringType()),
    types.StructField('iata', types.StringType()),
    types.StructField('icao', types.StringType()),
    types.StructField('callsign', types.StringType()),
    types.StructField('country', types.StringType()),
    types.StructField('active', types.StringType()),
])
#324,"All Nippon Airways","ANA All Nippon Airways","NH","ANA","ALL NIPPON","Japan","Y"

aircraft_schema = types.StructType([
    types.StructField('typecode', types.StringType()),
    types.StructField('aircraft_type', types.StringType()),
    types.StructField('airliner_type', types.StringType()),
    types.StructField('aircraft_category', types.StringType()),
    types.StructField('payload', types.StringType()),
])

def main(in_directory, out_directory):
    #Read in data file, keep flight number, aircraft type, origin, destination, date
    #Add airline icao for later join
    print("Data")
    dat = spark.read.csv(in_directory, header=True, schema=flight_schema)
    dat = dat.select("callsign", "typecode", "origin", "destination", "day")
    dat = dat.na.drop()
    dat = dat.withColumn("icao", dat.callsign.substr(1,3))

    #Read in airport lookup table
    print("Airports")
    airports = spark.read.csv(in_airport, header=True, schema=airport_schema)
    airports = airports.select("ident", "type", "continent", "iso_country")

    #Read in airline lookup table
    print("Airlines")
    airlines = spark.read.csv(in_airlines, schema=airline_schema)
    airlines = airlines.select("icao", "name")

    #Read in airline lookup table
    print("Airlines")
    aircraft = spark.read.csv(in_aircraft, schema=aircraft_schema)
    
    #========== Joins ==========#
    #Get origin/destination country and region
    #Get airline name
    print("Joining")
    #Origin Airport
    joined = dat.join(airports, on=(dat['origin'] == airports['ident']))
    joined = joined.withColumnRenamed("continent","origin_continent") \
    .withColumnRenamed("iso_country","origin_country") \
    .withColumnRenamed("type", "origin_airport_type")
    joined = joined.drop("ident")

    #Destination Airport
    joined = joined.join(airports, on=(dat['destination'] == airports['ident']))
    joined = joined.withColumnRenamed("continent","destination_continent") \
    .withColumnRenamed("iso_country","destination_country") \
    .withColumnRenamed("type", "desitination_airport_type")
    joined = joined.drop("ident")

    #Airline Names
    joined = joined.join(airlines, on="icao")
    joined = joined.withColumnRenamed("name", "airline_name")

    #Aircraft Info
    joined = joined.join(aircraft, on="typecode")
    joined = joined.drop("typecode")

    joined = joined.na.drop()

    #Rearrange columns
    joined = joined.select("callsign", "icao", "airline_name", "aircraft_type", "airliner_type", "aircraft_category", "origin", "origin_airport_type", "origin_continent", "origin_country", "destination", "desitination_airport_type", "destination_continent", "destination_country", "day")
    # joined.show()
    
    # Used to generate list of unique aircraft for later analysis.
    # You shouldn't need to uncomment unless you want to check aircraft.csv    
    # print("Unique")
    # unique = joined.select("typecode").distinct()
    # unique.write.csv("unique.csv", mode='overwrite')

    print("Writing")
    joined.write.parquet(out_directory, mode='overwrite')

if __name__=='__main__':
    in_directory = sys.argv[1]
    in_airport = sys.argv[2]
    in_airlines = sys.argv[3]
    in_aircraft = sys.argv[4]
    out_directory = sys.argv[5]
    main(in_directory, out_directory)
