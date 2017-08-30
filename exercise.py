#coding=utf-8

from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from math import sin, cos, sqrt, atan2, radians
import math

def distance(data_lat, data_lon, poi_lat, poi_lon):
	"""
    Define a function for calculating the distance between POI and user location.
    R is the radius of earth in kilometer. 
    Input: User Latitude, User Longitude, POI Latitude, POI Longitude
    Output: Distance
	"""    
    R = 6371.0

    lat1 = radians(data_lat)
    lon1 = radians(data_lon)
    lat2 = radians(poi_lat)
    lon2 = radians(poi_lon)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    
    return distance


""" Load DataSample.csv and POIList.csv into Dataframe """
df = sqlContext.read.format('com.databricks.spark.csv')\
                    .options(header='true', inferschema='true')\
                    .load("/Users/Jianyu/Desktop/work-samples-master/data-mr/data/DataSample.csv")

df_poi = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(header='true', inferschema='true')\
                        .load("/Users/Jianyu/Desktop/work-samples-master/data-mr/data/POIList.csv")

""" Cleaning: Remove the records that identical geoinfo and timest """
df_data = df.dropDuplicates([' TimeSt','Latitude', 'Longitude'])

""" Calculate distance between each request and each POI """
udf_dict = udf(distance, FloatType())
df_data_with_poi = df_data.join(df_poi)\
                   .withColumn('distance', udf_dict(df_data['Latitude'], df_data['Longitude'], df_poi[' Latitude'], df_poi['Longitude']))\
                   .select('_Id', 'POIID', 'distance')

""" Find the minimum distance for each request """
min_distances = df_data_with_poi.groupBy('_Id').agg({'distance': 'min'})

""" Assign each request in the `DataSample.csv` to one of those POI locations that has minimum distance to the request location """
df5 = df_data_with_poi\
      .join(min_distances, (df_data_with_poi['_Id'] == min_distances['_Id']) & (df_data_with_poi['distance'] == min_distances['MIN(distance)']))\
      .select(df_data_with_poi['_Id'], df_data_with_poi['POIID'], df_data_with_poi['distance'])

""" As POI1 and POI2 have the same latitude and longitude, requests which are paired with POI1 or POI2 will be duplicated."""
df6 = df5.dropDuplicates(['_Id'])

summary = df6.describe(['distance']).collect()

mean = summary[1][1]
stddev = summary[2][1]

""" Find the radius and count for each POI """
radius = df6.groupBy('POIID').agg({'distance': 'max'}).collect()
record_count = df6.groupBy(['POIID']).count().collect()


















