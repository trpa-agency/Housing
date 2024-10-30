import pandas as pd

def join_census_data(parcel_dataframe, census_dataframe):
    # Merge the dataframes
    # This would take a pivoted census dataframe and merge it with the parcel dataframe
    merged = pd.merge(parcel_dataframe, census_dataframe, how='left', left_on='TRPAID', right_on='TRPAID')
    return merged