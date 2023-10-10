import geopandas as gpd
import csv
from shapely.geometry import Polygon, MultiPolygon
import sys
from shapely.geometry import Point, Polygon
import pandas as pd

# Specify the path to your shapefile (replace with your actual path)
shapefile_path = '/Users/rahulrahul/Downloads/GeoSureHexGrids-2/Data/GB_Hex_5km_GS_Landslides_v8.shp'


# Read the shapefile
gdf = gpd.read_file(shapefile_path)

output_csv_path = '/Users/rahulrahul/Documents/Python/Data/Landslide_raw_data.csv'
gdf.to_csv(output_csv_path, index=False)
with open('/Users/rahulrahul/Documents/Python/Data/Landslide_raw_data.csv', 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)

    polygons = []
    legend_values = []  # List to store 'Legened' values

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

        # Store 'Legened' values
        legend_values.append(row['Legend'])

# Create GeoDataFrame including 'Legened' and geometry columns
polygon_data = {'geometry': polygons, 'Legend': legend_values}
polygon_gdf = gpd.GeoDataFrame(polygon_data)

print(polygon_gdf.head(10))

polygon_gdf.reset_index(drop=True, inplace=True)

coordinates_data = pd.read_csv('/Users/rahulrahul/Documents/Python/Data/main_dataset.csv')
# Sample coordinates DataFrame


coordinates_df = pd.DataFrame(coordinates_data)

# Create Point objects from coordinates
geometry = [Point(x, y) for x, y in zip(coordinates_df['Easting'], coordinates_df['Northing'])]
coordinates_gdf = gpd.GeoDataFrame(coordinates_df, geometry=geometry)

# Perform spatial join to check if coordinates lie within polygons
result = gpd.sjoin(coordinates_gdf, polygon_gdf[['Legend', 'geometry']], how="left", predicate="within")

result.rename(columns={'geometry': 'polygon_geometry'}, inplace=True)
result = result.drop(columns=['polygon_geometry'])
# Merge the result DataFrame with polygon_gdf based on the index
merged_result = result.merge(polygon_gdf[['geometry']], left_on='index_right', right_index=True, how="outer")

# Export the merged GeoDataFrame to a CSV file

output_csv_path = '/Users/rahulrahul/Documents/Python/Data/Landslide_final_data.csv'
merged_result.to_csv(output_csv_path, index=True)

print(merged_result)