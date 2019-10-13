import numpy as np
import pandas as pd


# Define a function to get a filename by chunks
def green_trips_chuks(file_name, chunk_size=100000):
    final_df = list()

    for chunk in pd.read_csv(file_name,
                             compression="gzip",
                             chunksize=chunk_size):

        chunk["pickup_datetime"] = (
            chunk.pickup_datetime.values.astype("datetime64[ns]"))
        chunk["dropoff_datetime"] = (
            chunk.dropoff_datetime.values.astype("datetime64[ns]"))
        chunk["pickup_month"] = chunk.pickup_datetime.dt.month
        chunk["pickup_hour"] = chunk.pickup_datetime.dt.hour
        chunk["pickup_time"] = chunk.pickup_datetime.dt.time
        chunk["dropoff_time"] = chunk.dropoff_datetime.dt.time

        final_df.append(chunk)
        df = pd.concat(final_df)

    return df


# Define a function to process mta_trips_data
def mta_trips_chuks(file_name, chunk_size=100000):
    final_df = list()

    for chunk in pd.read_csv(file_name,
                             compression="gzip",
                             chunksize=chunk_size):

        chunk["datetime"] = pd.to_datetime(chunk.datetime)
        chunk["new_entries"] = chunk.new_entries * 1000000
        chunk["new_exits"] = chunk.new_exits * 1000000
        chunk["month"] = chunk.datetime.dt.month
        chunk["hour"] = chunk.datetime.dt.hour
        chunk["time"] = chunk.datetime.dt.time

        final_df.append(chunk)

        df = pd.concat(final_df)
        df["station"] = df.station.astype("category")
        df["line_name"] = df.line_name.astype("category")
        df["division"] = df.division.astype("category")
        df["audit_type"] = df.audit_type.astype("category")
        df["unit_id"] = df.unit_id.astype("category")

    return df


green_trips = green_trips_chuks("./datathon/data/raw/green_trips.csv.gz")
mta_trips = mta_trips_chuks("./datathon/data/raw/mta_trips.csv.gz")

print(green_trips.info())
print(mta_trips.info())
