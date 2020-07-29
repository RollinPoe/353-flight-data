# 353-flight-data
For CMPT353-Computational Data Science's Final Project

### Data Sources:
<<<<<<< HEAD
* Flight Data: [OpenSky](https://opensky-network.org/community/blog/item/6-opensky-covid-19-flight-dataset) ([Direct Link](https://zenodo.org/record/3901482)) 17 files (2019-01 --> 2020-05) as csv.gz. Not included in git due to size.
=======
* Flight Data: [OpenSky](https://opensky-network.org/community/blog/item/6-opensky-covid-19-flight-dataset) ([Direct Link](https://zenodo.org/record/3901482))
>>>>>>> 63d73ddb1964abd953a80553741fdcb7a9a04770
* Airport Lookup: [OurAirports](https://ourairports.com/data/?spm=a2c6h.14275010.0.0.4c494a74QoD9gH)
* Airline Lookup: [Openflights](https://openflights.org/data.html#airline)

### How To Use
* **01_flight_etl.py** - Takes input directory, airport lookup, airline lookup, output directory. Returns parquet files
  * `spark-submit 01_flight_etl.py input_data airports.csv airlines.csv output`
