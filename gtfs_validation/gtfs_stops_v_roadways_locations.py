
#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd
import os
from shapely.geometry import Point

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# Paths to input files
roadways_path = r'path\to\your\roadways.shp'  # Replace with your roadways shapefile path
gtfs_folder = r'path\to\your\GTFS\folder'     # Replace with your GTFS folder path
stops_path = os.path.join(gtfs_folder, 'stops.txt')
output_dir = r'path\to\output\directory'       # Replace with your desired output directory

# Coordinate Reference Systems
stops_crs = 'EPSG:4326'   # WGS84 Latitude/Longitude
target_crs = 'EPSG:2283'  # NAD83 / Virginia North

# Negative buffer distances in feet
buffer_distances = [-1, -5, -10]  # Adjust buffer distances as needed

# Output file names
output_shp_name = 'intersecting_stops.shp'
output_csv_name = 'intersecting_stops.csv'

# ==============================
# END OF CONFIGURATION SECTION
# ==============================

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Verify that stops.txt exists in the provided GTFS folder
if not os.path.isfile(stops_path):
    raise FileNotFoundError(f"'stops.txt' not found in the GTFS folder: {gtfs_folder}")

# Read roadways shapefile
roadways_gdf = gpd.read_file(roadways_path)

# Read GTFS stops.txt into DataFrame
stops_df = pd.read_csv(stops_path)

# Create GeoDataFrame from stops_df
geometry = [Point(xy) for xy in zip(stops_df.stop_lon, stops_df.stop_lat)]
stops_gdf = gpd.GeoDataFrame(stops_df, geometry=geometry)

# Set CRS for stops_gdf
stops_gdf.set_crs(stops_crs, inplace=True)

# Reproject both datasets to the target CRS
roadways_gdf = roadways_gdf.to_crs(target_crs)
stops_gdf = stops_gdf.to_crs(target_crs)

# Perform spatial join to find stops that intersect roadways
intersecting_stops = gpd.sjoin(stops_gdf, roadways_gdf, how='inner', predicate='intersects')

# Optional: keep only columns from stops_gdf
intersecting_stops = intersecting_stops[stops_gdf.columns]

# Add 'x' and 'y' columns for coordinate reference
intersecting_stops['x'] = intersecting_stops.geometry.x
intersecting_stops['y'] = intersecting_stops.geometry.y

# Function to determine depth of conflict
def determine_conflict_depth(stops_gdf, roadways_gdf, buffer_distances):
    for buffer_distance in buffer_distances:
        # Buffer the roadways by the negative distance
        roadways_buffered = roadways_gdf.copy()
        roadways_buffered['geometry'] = roadways_buffered.buffer(buffer_distance)

        # Remove invalid or empty geometries
        roadways_buffered = roadways_buffered[~roadways_buffered.is_empty]
        roadways_buffered = roadways_buffered[roadways_buffered.is_valid]

        # Spatial join to find stops that intersect the buffered roadways
        buffered_join = gpd.sjoin(stops_gdf, roadways_buffered[['geometry']], how='left', predicate='intersects')

        # Create a column to indicate whether the stop intersects the buffered roadways
        column_name = f'conflict_{-buffer_distance}ft'
        stops_gdf[column_name] = ~buffered_join['index_right'].isnull()
    return stops_gdf

# Determine depth of conflict and update intersecting_stops
intersecting_stops = determine_conflict_depth(intersecting_stops, roadways_gdf, buffer_distances)

# Sort by conflict depth columns in descending order
conflict_columns = [f'conflict_{-bd}ft' for bd in buffer_distances]
intersecting_stops = intersecting_stops.sort_values(
    by=conflict_columns,
    ascending=[False]*len(conflict_columns)
)

# Save to shapefile
output_shp_path = os.path.join(output_dir, output_shp_name)
intersecting_stops.to_file(output_shp_path)

# Save to CSV
output_csv_path = os.path.join(output_dir, output_csv_name)
intersecting_stops.to_csv(output_csv_path, index=False)

print("Processing complete. Output saved to:", output_dir)








