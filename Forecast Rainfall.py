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

data={'postcode':['SW13 9HP','YO30 5RX','BN5 9SL','BS1 3FF','B23 7SJ','NN1 3ND','M1 1BE','NE1 1EZ',
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
    base_dir = '/Users/rahulrahul/Documents/Python/Data/Projected_Rainfall'

    request_url = request_url = 'https://ukclimateprojections-ui.metoffice.gov.uk/wps?service=wps&request=Execute&version=1.0.0&Identifier=LS1_Subset_02&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=Area={};'\
                                'Collection=land-prob;DataFormat=csv;ReturnPeriod=rp20;Scenario=rcp85;TemporalAverage=djf;TimeSlice=2020%7C2061;Variable=pr1day'
    Area_value = row['Area']
    request_url = request_url.format(Area_value)
    outputs_dir = os.path.join(base_dir)
    cli.submit(request_url, outputs_dir=outputs_dir)
    for filename in os.listdir(base_dir):
        if filename.endswith(".csv"):
            csv_file_path = os.path.join(base_dir, filename)
            Area = pd.read_csv(csv_file_path, nrows=1,skiprows=range(0, 1), index_col=0,names=['Date', 'Area'])
            transposed_df = pd.read_csv(csv_file_path, header=None, skiprows=16)
            transposed_df.columns = range(1, len(transposed_df.columns) + 1)
            selected_columns = [1, 16]
            selected_df = transposed_df[selected_columns]
            new_column_names = ['New_Column_1', 'New_Column_16']
            selected_df.columns = new_column_names
            selected_df['New_Column_1'] = pd.to_datetime(selected_df['New_Column_1'])
            selected_df['Year'] = selected_df['New_Column_1'].dt.year
            selected_df.drop(columns=['New_Column_1'], inplace=True)
            transposed_selected_df = selected_df.transpose()
            transposed_selected_df.columns = transposed_selected_df.iloc[1]
            transposed_selected_df = transposed_selected_df[0:]
            suffix = "_FR_10"
            transposed_selected_df.columns = [f"{col}{suffix}" for col in transposed_selected_df.columns]
            transposed_selected_df_10pc = transposed_selected_df.drop(transposed_selected_df.index[1])


            start_concat_10= pd.concat([transposed_selected_df_10pc, Area], axis=1)
            start_concat_10 = start_concat_10.drop(columns=start_concat_10.columns[0])
            start_concat_10['Area'] = start_concat_10['Area'].shift(-1)
            start_concat_10 = start_concat_10.drop(start_concat_10.index[1])
            start_concat_10 = start_concat_10.drop(columns=start_concat_10.columns[0])
            start_concat_10 = start_concat_10.reset_index(drop=True)


            # Code to read the CSV file and read only the column for 50PC and transpose it , by adding header names with a suffix
            selected_columns = [1, 56]
            selected_df = transposed_df[selected_columns]
            new_column_names = ['New_Column_1', 'New_Column_16']
            selected_df.columns = new_column_names
            selected_df['New_Column_1'] = pd.to_datetime(selected_df['New_Column_1'])
            selected_df['Year'] = selected_df['New_Column_1'].dt.year
            selected_df.drop(columns=['New_Column_1'], inplace=True)
            transposed_selected_df = selected_df.transpose()
            transposed_selected_df.columns = transposed_selected_df.iloc[1]
            transposed_selected_df = transposed_selected_df[0:]
            suffix = "_FR_50"
            transposed_selected_df.columns = [f"{col}{suffix}" for col in transposed_selected_df.columns]
            transposed_selected_df_50pc = transposed_selected_df.drop(transposed_selected_df.index[1])

            # Concatenate Area dataframe to get the easting and Northing,
            start_concat_50= pd.concat([transposed_selected_df_50pc, Area], axis=1)
            start_concat_50 = start_concat_50.drop(columns=start_concat_50.columns[0])
            start_concat_50['Area'] = start_concat_50['Area'].shift(-1)
            start_concat_50 = start_concat_50.drop(start_concat_50.index[1])
            start_concat_50 = start_concat_50.drop(columns=start_concat_50.columns[0])
            start_concat_50 = start_concat_50.reset_index(drop=True)



            # Code to read the CSV file and read only the column for 50PC and transpose it , by adding header names with a suffix
            selected_columns = [1, 96]
            selected_df = transposed_df[selected_columns]
            new_column_names = ['New_Column_1', 'New_Column_16']
            selected_df.columns = new_column_names
            selected_df['New_Column_1'] = pd.to_datetime(selected_df['New_Column_1'])
            selected_df['Year'] = selected_df['New_Column_1'].dt.year
            selected_df.drop(columns=['New_Column_1'], inplace=True)
            transposed_selected_df = selected_df.transpose()
            transposed_selected_df.columns = transposed_selected_df.iloc[1]
            transposed_selected_df = transposed_selected_df[0:]
            suffix = "_FR_50"
            transposed_selected_df.columns = [f"{col}{suffix}" for col in transposed_selected_df.columns]
            transposed_selected_df_90pc = transposed_selected_df.drop(transposed_selected_df.index[1])


            # Concatenate Area dataframe to get the easting and Northing,
            start_concat_90= pd.concat([transposed_selected_df_90pc, Area], axis=1)
            start_concat_90 = start_concat_90.drop(columns=start_concat_90.columns[0])
            start_concat_90['Area'] = start_concat_90['Area'].shift(-1)
            start_concat_90 = start_concat_90.drop(start_concat_90.index[1])
            start_concat_90 = start_concat_90.drop(columns=start_concat_90.columns[0])
            start_concat_90 = start_concat_90.reset_index(drop=True)

            merged_df = pd.merge(start_concat_10, start_concat_50, on="Area")
            merged_df = pd.merge(merged_df, start_concat_90, on="Area")
            merged_df = merged_df.iloc[0:1]
            
            output_csv_path = '/Users/rahulrahul/Documents/Python/Data/Forecasted_rainfall_output.csv'
            #output_csv_path1 = '/Users/rahulrahul/Documents/Python/ClimateRiskFinal/ukcp-api-client/1daypr/rainfall.csv'
            #merged_df1.to_csv(output_csv_path1)
            merged_df.to_csv(output_csv_path, mode='a', header=False, index=False)
            #df1 = pd.concat([df1, merged_df1], ignore_index = True)
            try:
                shutil.rmtree(base_dir)
                print(f"Folder '{base_dir}' deleted successfully.")
            except OSError as e:
                print(f"Error deleting folder '{base_dir}': {e}")
            
