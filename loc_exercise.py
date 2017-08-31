#coding=utf-8

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from math import sin, cos, sqrt, atan2, radians
import math

def distance(data_lat, data_lon, poi_lat, poi_lon):

    """
    Usage: Calculating the distance between POI and user location.
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

def scale(density):
    """ 
	Usage: Scale density into -10 to 10
	Normalize data into [0,1]
	Input: POI density list
	Output: New scaled density list
    """
    density_scale = []
    for x in density:
        new = (((x - min(density)) / (max(density) - min(density))) - 0.5) * 20
        density_scale.append(new)
    print density_scale


if __name__ == "__main__":

    sc = SparkContext(appName="Location_Exercise")
    sqlContext = SQLContext(sc)

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
                       .select('_ID', 'POIID', 'distance')

    """ Find the minimum distance for each request """
    min_distances = df_data_with_poi.groupBy('_ID').agg({'distance': 'min'})

    """ Assign each request in the `DataSample.csv` to one of those POI locations that has minimum distance to the request location """
    df5 = df_data_with_poi\
          .join(min_distances, (df_data_with_poi['_ID'] == min_distances['_ID']) & (df_data_with_poi['distance'] == min_distances['MIN(distance)']))\
          .select(df_data_with_poi['_ID'], df_data_with_poi['POIID'], df_data_with_poi['distance'])

    """ As POI1 and POI2 have the same latitude and longitude, requests which are paired with POI1 or POI2 will be duplicated."""
    df6 = df5.dropDuplicates(['_ID'])
    print df6.collect()

    """ Find the mean and standard deviation of minimum distance """
    summary = df6.describe(['distance']).collect()

    mean = summary[1][1]
    stddev = summary[2][1]

    """ Find the radius and count for each POI """
    radius = df6.groupBy('POIID').agg({'distance': 'max'}).collect()
    record_count = df6.groupBy(['POIID']).count().collect()

    poi1_radius = radius[0][1]
    poi1_count = record_count[0][1]
    poi3_radius = radius[1][1]
    poi3_count = record_count[1][1]
    poi4_radius = radius[2][1]
    poi4_count = record_count[2][1]

    """ Calculate density by count/area """
    poi1_density = poi1_count / (math.pi * (poi1_radius ** 2))
    poi3_density = poi3_count / (math.pi * (poi3_radius ** 2))
    poi4_density = poi4_count / (math.pi * (poi4_radius ** 2))

    print "The density of POI1 is %f" % poi1_density
    print "The density of POI3 is %f" % poi3_density
    print "The density of POI4 is %f" % poi4_density

    density = [poi1_density, poi3_density, poi4_density]

    scale(density)

    sc.stop()





















