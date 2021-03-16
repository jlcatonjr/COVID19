import pandas as pd
import geopandas as gpd
import geoparquet as gpq
import numpy as np
import pyarrow as pa

def write_to_parquet(df, filename):
    table = pa.Table.from_pandas(df)
    pa.parquet.write_table(table, filename)

def import_geo_data(filename, index_col = "Date", FIPS_name = "FIPS"):
    # import county level shapefile
    map_data = gpd.read_file(filename = filename,                                   
                                   index_col = index_col)
    # rename fips code to match variable name in COVID-19 data
    map_data.rename(columns={"State":"state"},
                    inplace = True)
    # Combine statefips and county fips to create a single fips value
    # that identifies each particular county without referencing the 
    # state separately
    map_data[FIPS_name] = map_data["STATEFP"].astype(str) + \
        map_data["COUNTYFP"].astype(str)
    map_data[FIPS_name] = map_data[FIPS_name].astype(np.int64)
    # set FIPS as index
    map_data.set_index(FIPS_name, inplace=True)
    
    return map_data

covid_data =pd.read_parquet("../COVID19DataForVoila.parquet.gzip")
state_dict = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ',
    'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 
    'Delaware': 'DE', 'District of Columbia': 'DC', 'Florida': 'FL', 
    'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL',
    'Indiana': 'IN', 'Iowa': 'IA','Kansas': 'KS', 'Kentucky': 'KY',
    'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
    'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH',
    'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC',
    'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI',
    'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX',
    'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA',
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}
state_df = covid_data.reset_index()[["Total Cases", 
                                         "Total Deaths", 
                                         "total_population", 
                                         "state", 
                                         "date"]].copy()

state_df = state_df.groupby(["state", "date"]).sum()
US_df =state_df.groupby("date").sum()
US_df["state"] = "United States"
US_df = US_df.reset_index().set_index(["state", "date"])
state_df = state_df.append(US_df)

for key in ["Cases", "Deaths"]:
    state_df[key + " per Million"] = state_df[
    "Total " + key].div(state_df["total_population"]) * 10 ** 6
    state_df["Daily " + key] = state_df[
        "Total " + key].groupby("state").diff(1)
    state_df["Daily " + key+ " 7 Day MA"]= state_df["Daily " + key].rolling(7).mean() 
    state_df["Daily " + key+ " per Million 7 Day MA"] = state_df["Daily " + key+ " 7 Day MA"]\
        .div(state_df["total_population"])* 10 ** 6

state_df.fillna(0, inplace = True)
state_df.reset_index(inplace = True)
state_df.set_index(["date", "state"], inplace = True)
state_df_pivot = pd.pivot_table(state_df.reset_index(), 
               values=['Cases per Million', "Deaths per Million",
                       "Daily Cases per Million 7 Day MA", "Daily Deaths per Million 7 Day MA",
                      "Total Cases", "Total Deaths",
                      "Daily Cases", "Daily Deaths",
                      "Daily Cases 7 Day MA", "Daily Deaths 7 Day MA"], 
               index=['date'], columns=['state'])

covid_data = pd.pivot_table(covid_data.reset_index(), 
               values=['Cases per Million', "Deaths per Million",
                       "Daily Cases 7 Day MA", "Daily Deaths 7 Day MA",
                       "Daily Cases per Million 7 Day MA", "Daily Deaths per Million 7 Day MA",
                       "Daily Cases", "Daily Deaths",
                      "Total Cases", "Total Deaths"], 
               index=['fips_code'], columns=['date'])
    
map_data = import_geo_data(filename = "countiesWithStatesAndPopulation.shp",
    index_col = "date", FIPS_name= "fips_code")
counties = covid_data.index
covid_map_data= map_data.loc[counties]
for key, val in covid_data.items():
    covid_map_data[key] = val.astype(np.float64)
# transform tuple keys to string for saving, then transform back when imported
covid_map_data.rename(columns={col:str(col) for col in covid_map_data.keys()}, 
                      inplace = True)


write_to_parquet(state_df_pivot, "../COVID19StatePivot.parquet.gzip")
write_to_parquet(covid_data, "../COVID19CovidDataPivot.parquet.gzip")
# covid_map_data.to_file("../COVID19MapData.shp")