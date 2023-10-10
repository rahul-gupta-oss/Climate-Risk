import shutil
import pandas as pd
import os
from ukcp_api_client.client import UKCPApiClient
import pandas as pd
from geopy.geocoders import ArcGIS
nom=ArcGIS()
import pyproj
import math

api_key = os.environ['API_KEY']
cli = UKCPApiClient(api_key=api_key)
folder_path = 'Daily_rainfall_actual'

#df = pd.read_csv('/Users/rahulrahul/Downloads/ukpostcodes.csv', usecols=["postcode"])
 # df1=df.head()
#df1 = df.iloc[:1]

data={'postcode':['NE1 1EZ',
                   'EH1 1DU','CF10 5ET']}
df1 = pd.DataFrame(data)

df1["Longitude"] = df1["postcode"].apply(nom.geocode).apply(lambda x:(str(x.latitude)))
df1["Latitude"] = df1["postcode"].apply(nom.geocode).apply(lambda x:(str(x.longitude)))

def convert_coordinates(df):
    # Create projection objects for WGS84 (latitude/longitude) and Airy (easting/northing)
    wgs84 = pyproj.Proj("+proj=latlong +datum=WGS84 +ellps=WGS84")
    british_national_grid = pyproj.Proj("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +no_defs")

    # Create empty lists to store the converted coordinates
    eastings = []
    northings = []

    # Iterate through each row of the DataFrame
    for index, row in df1.iterrows():
        lat = row['Latitude']  # Latitude column name in the DataFrame
        lon = row['Longitude']  # Longitude column name in the DataFrame

        # Convert latitude and longitude to easting and northing using the Airy projection
        easting, northing = pyproj.transform(wgs84, british_national_grid, lat, lon)

        # Append the converted coordinates to the respective lists
        eastings.append(easting)
        northings.append(northing)
    def round_to_nearest_lower_half(x):
        return math.floor(x + 0.5) - 0.5
    # Add the converted coordinates as new columns in the DataFrame
    df1['Easting'] = eastings
    df1['Northing'] = northings
    df1['Easting'] = df['Easting'].round(0)
    df1['Easting'] = (df['Easting']/25000)
    df['Easting'] = df['Easting'].apply(round_to_nearest_lower_half)
    df1['Easting'] =df1['Easting']*25000
    
    
    df1['Northing'] = df['Northing'].round(0)
    df1['Northing'] = (df['Northing']/25000)
    df1['Northing'] = df['Northing'].apply(round_to_nearest_lower_half)
    df1['Northing'] =df1['Northing']*25000
    
    
   
    return df1


df1 = convert_coordinates(df1)

df1['Area']='point%7C' + df1['Easting'].astype(str) + '%7C' + df1['Northing'].astype(str)
df1['Rainfall']=df1['Easting'].astype(str) + ' ' + df1['Northing'].astype(str)
df_no_duplicates = df1.drop_duplicates(subset='Rainfall')

for index, row in df_no_duplicates.iterrows():
    # Download the actual data for maximum rainfall and temperature for last 20 years, if needed, change the parameters in the below URL
    
    # Rainfall data link
    base_dir = 'Daily_temperature_actual'

    request_url = 'https://ukclimateprojections-ui.metoffice.gov.uk/wps?service=wps&request=Execute&version=1.0.0&Identifier=LS6_Subset_02&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=Area={};Collection=land-obs_25;DataFormat=csv;TemporalAverage=day;TimeSlice=2000%7C2021;Variable=tasmax'
    Area_value = row['Area']
    request_url = request_url.format(Area_value)
    outputs_dir = os.path.join(base_dir)
    cli.submit(request_url, outputs_dir=outputs_dir)
    for filename in os.listdir(base_dir):
        if filename.endswith(".csv"):
            csv_file_path = os.path.join(base_dir, filename)
            df = pd.read_csv(csv_file_path, skiprows=1, names=['Date', 'Rainfall'])
            
            df=df.drop(df.index[:10])
            df['Rainfall'] = df['Rainfall'].astype(float)

            # Round the values to integers (optional step, if needed)
            #df['11'] = df['11'].round().astype(int)
            df['Date'] = pd.to_datetime(df['Date'])
            df['year'] = df['Date'].dt.year
            df.drop(columns=['Date'], inplace=True)
            max_values = df.groupby('year')['Rainfall'].transform('max')
            mask = df['Rainfall'] == max_values
            df_max_values = df[mask]
            new_columns = df_max_values['year']
            transposed_df = df_max_values.transpose()
            transposed_df.columns = new_columns
            suffix = "_DTA"  # Replace with your desired suffix
            transposed_df.columns = [str(col) + suffix for col in transposed_df.columns]
            
            
            transposed_df.index = transposed_df.index.astype(str)
            transposed_df = transposed_df.iloc[:-1]
            transposed_df1 = pd.read_csv(csv_file_path, nrows=1,skiprows=range(0, 1), index_col=0,names=['Date', 'Rainfall'])
            merged_df1 = pd.concat([transposed_df, transposed_df1], axis=1)
            merged_df1 = merged_df1.drop(columns=merged_df1.columns[0])
            merged_df1['Rainfall'] = merged_df1['Rainfall'].shift(-1)
            merged_df1 = merged_df1.drop(merged_df1.index[1])
            merged_df1 = merged_df1.drop(columns=merged_df1.columns[0])

            
            output_csv_path = '/Users/rahulrahul/Documents/Python/Data/Actual_Temperature_output.csv'
        
            
            merged_df1.to_csv(output_csv_path, mode='a', header=False, index=False)
            #df1 = pd.concat([df1, merged_df1], ignore_index = True)
            try:
                shutil.rmtree(base_dir)
                print(f"Folder '{base_dir}' deleted successfully.")
            except OSError as e:
                print(f"Error deleting folder '{base_dir}': {e}")
            print(merged_df1)
            
            
                 

#print(df1)
#output_csv_path = '/Users/rahulrahul/Documents/Python/Data/temperature_actual.csv'
#transposed_df = pd.read_csv(output_csv_path, header=None)
#new_column_names = [f"{year}_dra" for year in range(2001, 2001 + 21)]
#transposed_df.columns = new_column_names
#transposed_df.rename(columns={transposed_df.columns[-1]: 'Rainfall'}, inplace=True)
#Temp_actual = pd.merge(df1, transposed_df, on='Rainfall', how='left')
#print(Temp_actual)

    
    

            


            



