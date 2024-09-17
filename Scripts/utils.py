import pandas as pd
import numpy as np
from arcgis.features import FeatureLayer
import os
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import arcpy


def get_conn(db):
    # Get database user and password from environment variables on machine running script
    db_user             = os.environ.get('DB_USER')
    db_password         = os.environ.get('DB_PASSWORD')

    # driver is the ODBC driver for SQL Server
    driver              = 'ODBC Driver 17 for SQL Server'
    # server names are
    sql_12              = 'sql12'
    sql_14              = 'sql14'
    # make it case insensitive
    db = db.lower()
    # make sql database connection with pyodbc
    if db   == 'sde_tabular':
        connection_string = f"DRIVER={driver};SERVER={sql_12};DATABASE={db};UID={db_user};PWD={db_password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
    elif db == 'tahoebmpsde':
        connection_string = f"DRIVER={driver};SERVER={sql_14};DATABASE={db};UID={db_user};PWD={db_password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
    elif db == 'sde':
        connection_string = f"DRIVER={driver};SERVER={sql_12};DATABASE={db};UID={db_user};PWD={db_password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url)
    # else return None
    else:
        engine = None
    # connection file to use in pd.read_sql
    return engine


# Gets data with query from the TRPA server
def get_fs_data_query(service_url, query_params):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query(query_params)
    # Convert the query result to a list of dictionaries
    feature_list = query_result.features
    # Create a pandas DataFrame from the list of dictionaries
    all_data = pd.DataFrame([feature.attributes for feature in feature_list])
    # return data frame
    return all_data


# Gets data from the TRPA server
def get_fs_data(service_url):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query()
    # Convert the query result to a list of dictionaries
    feature_list = query_result.features
    # Create a pandas DataFrame from the list of dictionaries
    all_data = pd.DataFrame([feature.attributes for feature in feature_list])
    # return data frame
    return all_data


# Gets spatially enabled dataframe from TRPA server
def get_fs_data_spatial(service_url):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query().sdf
    return query_result


# Gets spatially enabled dataframe with query
def get_fs_data_spatial_query(service_url, query_params):
    feature_layer = FeatureLayer(service_url)
    query_result = feature_layer.query(query_params).sdf
    return query_result
def spatial_join_map(target, source, join_field,  map_field, target_field):
    # spatial join
    arcpy.SpatialJoin_analysis(target, source, 'memory\\temp', 
                               'JOIN_ONE_TO_ONE', 'KEEP_ALL','HAVE_THEIR_CENTER_IN')
    # get result as a spatial dataframe
    join = pd.DataFrame.spatial.from_featureclass('memory\\temp')
    join.info()
    # map values
    target[target_field] = target[join_field].map(dict(zip(join[join_field], join[map_field])))
    return target