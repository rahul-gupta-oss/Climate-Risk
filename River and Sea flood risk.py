
import geopandas as gpd
import csv
from shapely.geometry import Polygon, MultiPolygon
import sys
from shapely.geometry import Point, Polygon
import pandas as pd

#Uncomment the below code when running for the complete book

# Specify the path to your shapefile (replace with your actual path)
#shapefile_path = '/Users/rahulrahul/Downloads/EA_RiskOfFloodingFromRiversAndSea_SHP_Full 2/data/Risk_of_Flooding_from_Rivers_and_Sea.shp'
# Read the shapefile
#gdf = gpd.read_file(shapefile_path)
#output_csv_path = '/Users/rahulrahul/Documents/Python/Data/River_sea_data.csv'
#gdf.to_csv(output_csv_path, index=False)

csv.field_size_limit(sys.maxsize) 
with open('/Users/rahulrahul/Documents/Python/Data/River_sea_data.csv', 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)

    polygons = []
    prob_4band_values = []  # List to store 'prob_4band' values

    # Parse and create Polygon/MultiPolygon objects
    for row in csv_reader:
        polygon_string = row['geometry']
        
        # Check if it's a MultiPolygon
        if polygon_string.startswith("MULTI"):
            multi_coordinates_str = polygon_string.replace("MULTI", "").replace("(", "").replace(")", "")
            polygon_strings = multi_coordinates_str.split("),(")
            
            multi_polygons = []
            for poly_str in polygon_strings:
                coordinates_list = poly_str.split(", ")
                coordinates_tuples = []
                for coord in coordinates_list:
                    try:
                        x, y = map(float, coord.strip("()").split())
                        coordinates_tuples.append((x, y))
                    except ValueError:
                        print(f"Error processing: {coord}")
                if coordinates_tuples:
                    polygon_data = Polygon(coordinates_tuples)
                    multi_polygons.append(polygon_data)
            if multi_polygons:
                polygons.append(MultiPolygon(multi_polygons))
            else:
                print(f"Skipping invalid MultiPolygon: {polygon_string}")
        else:
            # Regular Polygon
            coordinates_str = polygon_string.replace("POLYGON ((", "").replace("))", "")
            coordinates_list = coordinates_str.split(", ")
            coordinates_tuples = []
            for coord in coordinates_list:
                try:
                    x, y = map(float, coord.strip("()").split())
                    coordinates_tuples.append((x, y))
                except ValueError:
                    print(f"Error processing: {coord}")
            if coordinates_tuples:
                polygon_data = Polygon(coordinates_tuples)
                polygons.append(polygon_data)
            else:
                print(f"Skipping invalid polygon: {polygon_string}")

        # Store 'prob_4band' values
        prob_4band_values.append(row['prob_4band'])

# Create GeoDataFrame including 'prob_4band' and geometry columns
polygon_data = {'geometry': polygons, 'prob_4band': prob_4band_values}
polygon_gdf = gpd.GeoDataFrame(polygon_data)

print(polygon_gdf.head(10))

polygon_gdf.reset_index(drop=True, inplace=True)

coordinates_data = pd.read_csv('/Users/rahulrahul/Documents/Python/Data/main_dataset.csv')
# Sample coordinates DataFrame


coordinates_df = pd.DataFrame(coordinates_data)

# Create Point objects from coordinates
geometry = [Point(x, y) for x, y in zip(coordinates_df['Easting'], coordinates_df['Northing'])]
coordinates_gdf = gpd.GeoDataFrame(coordinates_df, geometry=geometry)
print(polygon_gdf)
# Perform spatial join to check if coordinates lie within polygons
result = gpd.sjoin(coordinates_gdf, polygon_gdf[['prob_4band', 'geometry']], how="left", predicate="within")

# Export the merged GeoDataFrame to a CSV file
merged_result = result.merge(polygon_gdf[['geometry']], left_on='index_right', right_index=True, how="outer")
merged_result.dropna(subset=['prob_4band'], inplace=True)

selected_columns = ['postcode', 'prob_4band']

# Select and keep the desired columns
merged_result = merged_result[selected_columns]

output_csv_path = '/Users/rahulrahul/Documents/Python/Data/River_sea_final_data.csv'
merged_result.to_csv(output_csv_path, index=True)

print(merged_result)
    
    
            



