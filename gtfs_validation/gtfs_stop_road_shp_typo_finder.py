#!/usr/bin/env python
# coding: utf-8

# In[6]:


#!/usr/bin/env python
# coding: utf-8

import geopandas as gpd
import pandas as pd
import os
import re
from shapely.geometry import Point
from rapidfuzz import fuzz, process
from pyproj import CRS
import logging

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# Paths to input files
gtfs_folder = r'path\to\your\GTFS\folder'          # Replace with your GTFS folder path
stops_filename = 'stops.txt'
stops_path = os.path.join(gtfs_folder, stops_filename)

roadways_path = r'path\to\your\roadways.shp'      # Replace with your roadways shapefile path

# Output settings
output_dir = r'path\to\output\directory'          # Replace with your desired output directory
output_csv_name = 'potential_typos.csv'
output_csv_path = os.path.join(output_dir, output_csv_name)

# Coordinate Reference Systems
stops_crs = 'EPSG:4326'  # WGS84 Latitude/Longitude. This is the standard CRS for GTFS stop coordinates.
                          # You typically do not need to change this unless your stops data uses a different CRS.
target_crs = 'EPSG:2248' # Projected CRS for spatial analysis (adjust as needed).
                          # Replace with a CRS suitable for proximity calculations in your region.

# Processing parameters
similarity_threshold = 80                         # Similarity threshold for RapidFuzz (0-100), higher number will yield fewer results

# Buffer distance configuration
buffer_distance_value = 50                        # Numeric value for buffer distance
buffer_distance_unit = 'feet'                     # Unit for buffer distance: 'feet' or 'meters'

# Roadway Shapefile Column Configuration
required_columns_roadway = ['RW_PREFIX', 'RW_TYPE_US', 'RW_SUFFIX', 'RW_SUFFIX_', 'FULLNAME']
descriptions_roadway = {
    'RW_PREFIX': "Directional prefix (e.g., 'N' in 'N Washington St')",
    'RW_TYPE_US': "Street type (e.g., 'St' in 'N Washington St')",
    'RW_SUFFIX': "Directional suffix (e.g., 'SE' in 'Park St SE')",
    'RW_SUFFIX_': "Additional suffix (e.g., 'EB' in 'RT267 EB')",
    'FULLNAME': "Full street name"
}

# Optional: Exclude certain roadway types (modify as per your data)
# excluded_rw_types = ['Highway', 'Freeway']          # Replace with actual types present in your data
# Commented out the above line to remove road type filtering

# ==============================
# END OF CONFIGURATION SECTION
# ==============================

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Function to determine the linear unit of a CRS
def get_crs_unit(crs_code):
    try:
        crs = CRS.from_user_input(crs_code)
        if crs.axis_info:
            unit = crs.axis_info[0].unit_name  # Access the unit name of the first axis
            return unit
        else:
            logging.error("CRS has no axis information.")
            return None
    except Exception as e:
        logging.error(f"Error determining CRS unit: {e}")
        return None

# Function to convert buffer distance to target CRS units
def convert_buffer_distance(value, from_unit, to_unit):
    conversion_factors = {
        ('feet', 'meters'): 0.3048,
        ('meters', 'feet'): 3.28084,
        ('metre', 'feet'): 3.28084,
        ('us survey foot', 'meters'): 0.3048006096012192,
        ('meters', 'us survey foot'): 3.280833333333333,
        ('feet', 'us survey foot'): 0.999998,        # Added conversion
        ('us survey foot', 'feet'): 1.000002,        # Added conversion
        # Add more conversions if needed
    }
    key = (from_unit.lower(), to_unit.lower())
    if key in conversion_factors:
        return value * conversion_factors[key]
    else:
        raise ValueError(f"Conversion from {from_unit} to {to_unit} not supported.")

# Determine CRS unit
crs_unit = get_crs_unit(target_crs)
if crs_unit is None:
    raise ValueError("Unable to determine the CRS unit. Please check the target CRS.")

logging.info(f"Target CRS ({target_crs}) uses '{crs_unit}' as its linear unit.")

# Validate buffer distance unit
supported_units = ['feet', 'meters', 'metre', 'us survey foot']
if buffer_distance_unit.lower() not in supported_units:
    raise ValueError(f"Unsupported buffer distance unit '{buffer_distance_unit}'. Supported units are: {supported_units}")

# Convert buffer distance to target CRS units if necessary
if buffer_distance_unit.lower() != crs_unit.lower():
    try:
        buffer_distance = convert_buffer_distance(buffer_distance_value, buffer_distance_unit, crs_unit)
        logging.info(f"Buffer distance converted from {buffer_distance_unit} to {crs_unit}: {buffer_distance:.6f} {crs_unit}")
    except ValueError as ve:
        logging.error(ve)
        raise
else:
    buffer_distance = buffer_distance_value
    logging.info(f"Buffer distance: {buffer_distance} {crs_unit}")

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    logging.info(f"Created output directory: {output_dir}")

# Verify that stops.txt exists in the provided GTFS folder
if not os.path.isfile(stops_path):
    raise FileNotFoundError(f"'stops.txt' not found in the GTFS folder: {gtfs_folder}")

# Read GTFS stops.txt into DataFrame
stops_df = pd.read_csv(stops_path, dtype=str)  # Read all columns as strings

# Required columns and their descriptions
required_columns_stops = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
descriptions_stops = {
    'stop_id': 'Unique identifier for the stop',
    'stop_name': 'Name of the stop',
    'stop_lat': 'Latitude of the stop',
    'stop_lon': 'Longitude of the stop'
}

# Column mapping for stops.txt (assumes columns are correctly named; adjust if necessary)
column_mapping_stops = {col: col for col in required_columns_stops if col in stops_df.columns}

# Check for missing columns and handle accordingly
missing_cols_stops = [col for col in required_columns_stops if col not in column_mapping_stops]
if missing_cols_stops:
    raise ValueError(f"The following required columns are missing in stops.txt: {missing_cols_stops}")

# Rename the columns in stops_df if necessary
stops_df = stops_df.rename(columns=column_mapping_stops)

# Convert stop_lat and stop_lon to float
stops_df['stop_lat'] = stops_df['stop_lat'].astype(float)
stops_df['stop_lon'] = stops_df['stop_lon'].astype(float)

# Create a GeoDataFrame with geometry
stops_gdf = gpd.GeoDataFrame(
    stops_df,
    geometry=gpd.points_from_xy(stops_df['stop_lon'], stops_df['stop_lat']),
    crs=stops_crs
)

# Read roadways shapefile
roadways_gdf = gpd.read_file(roadways_path)

# Reproject both GeoDataFrames to the target CRS
stops_gdf = stops_gdf.to_crs(target_crs)
roadways_gdf = roadways_gdf.to_crs(target_crs)

# Ensure the required columns are present in roadway shapefile
column_mapping_roadway = {}
for col in required_columns_roadway:
    if col in roadways_gdf.columns:
        column_mapping_roadway[col] = col
    else:
        logging.warning(f"The column '{col}' is missing from the roadway shapefile.")
        logging.info(f"Description: {descriptions_roadway[col]}")
        logging.info(f"Available columns: {roadways_gdf.columns.tolist()}")
        new_col = input(f"Please enter the correct column name for '{col}' (or leave blank to skip): ").strip()
        while new_col and new_col not in roadways_gdf.columns:
            logging.warning(f"'{new_col}' is not among the available columns: {roadways_gdf.columns.tolist()}")
            new_col = input(f"Please enter the correct column name for '{col}' (or leave blank to skip): ").strip()
        if new_col:
            column_mapping_roadway[col] = new_col
            logging.info(f"Mapped '{col}' to '{new_col}'")
        else:
            logging.info(f"Skipped mapping for '{col}'")

# Remove columns that are not mapped
column_mapping_roadway = {k: v for k, v in column_mapping_roadway.items() if v is not None}

# Check if 'FULLNAME' is present after mapping and mapped to a valid column
if 'FULLNAME' not in column_mapping_roadway or not column_mapping_roadway['FULLNAME']:
    raise ValueError("The 'FULLNAME' column is required in the roadway shapefile.")

# Rename the columns in roadway_gdf
roadways_gdf = roadways_gdf.rename(columns=column_mapping_roadway)

# Optional: Exclude certain roadway types to reduce irrelevant comparisons
# Removed the following block to eliminate road type filtering
# if excluded_rw_types:
#     original_count = roadways_gdf.shape[0]
#     roadways_gdf = roadways_gdf[~roadways_gdf['RW_TYPE_US'].isin(excluded_rw_types)]
#     excluded_count = original_count - roadways_gdf.shape[0]
#     logging.info(f"Excluded {excluded_count} roadways based on RW_TYPE_US")

# Extract unique modifiers from roadway data
modifiers_fields = ['RW_TYPE_US']  # Modify this list if needed
modifiers = set()

for field in modifiers_fields:
    mapped_field = column_mapping_roadway.get(field)
    if mapped_field and mapped_field in roadways_gdf.columns:
        unique_values = roadways_gdf[mapped_field].dropna().unique()
        modifiers.update(unique_values)

# Convert modifiers to lowercase and remove any NaN values
modifiers = set(str(mod).lower().strip() for mod in modifiers if pd.notnull(mod) and str(mod).strip())

logging.info(f"Extracted Modifiers ({len(modifiers)}): {modifiers}")

# Normalize 'FULLNAME' in roadways_gdf to create 'FULLNAME_clean'
def normalize_street_name(name, modifiers_set):
    if pd.isnull(name) or not isinstance(name, str):
        return ''
    # Remove modifiers if any modifiers are present
    if modifiers_set:
        pattern = r'\b(' + '|'.join(re.escape(mod) for mod in modifiers_set) + r')\b'
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    # Remove extra spaces and convert to lowercase
    return re.sub(r'\s+', ' ', name).strip().lower()

# Apply normalization to 'FULLNAME' in roadways_gdf
roadways_gdf['FULLNAME_clean'] = roadways_gdf['FULLNAME'].apply(lambda x: normalize_street_name(x, modifiers))

# Buffer stops by user-defined distance (in target CRS units)
stops_gdf['buffered_geometry'] = stops_gdf.geometry.buffer(buffer_distance)

# Set 'buffered_geometry' as the active geometry
stops_buffered_gdf = stops_gdf.set_geometry('buffered_geometry')

# Optional: Visualize a sample buffer and roadways to verify (requires matplotlib)
# Uncomment the following lines to plot
# import matplotlib.pyplot as plt
# sample_stop = stops_buffered_gdf.iloc[0]
# ax = roadways_gdf.plot(color='blue', figsize=(10, 10))
# sample_stop.buffered_geometry.plot(ax=ax, color='red', alpha=0.5)
# plt.show()

# Perform the spatial join using the 'intersects' predicate
joined_gdf = gpd.sjoin(
    stops_buffered_gdf[['stop_id', 'stop_name', 'buffered_geometry']],
    roadways_gdf[['FULLNAME', 'FULLNAME_clean', 'geometry']],
    how='left',
    predicate='intersects'
)

logging.info(f"Total stops processed: {len(stops_gdf)}")
logging.info(f"Total spatial join matches: {joined_gdf.shape[0]}")

# Function to extract street names from stop_name
def extract_street_names(stop_name):
    if pd.isnull(stop_name) or not isinstance(stop_name, str):
        return []
    separators = [' @ ', ' and ', ' & ', '/', ' intersection of ']
    pattern = '|'.join(map(re.escape, separators))
    streets = re.split(pattern, stop_name, flags=re.IGNORECASE)
    return [normalize_street_name(street, modifiers) for street in streets if street]

# Prepare road names for comparison from roadways_gdf
road_names_clean = set(roadways_gdf['FULLNAME_clean'].dropna().unique())

# Function to compare stop street names to road names
def compare_stop_to_roads(stop_id, stop_name, stop_streets, road_names, roadways_gdf, threshold):
    potential_typos = []
    for street in stop_streets:
        if street in road_names:
            continue
        else:
            # Get top 3 matches
            match_tuples = process.extract(street, road_names, scorer=fuzz.token_set_ratio, limit=3)
            # Loop through the top matches and add them to the output if they meet the score threshold
            for match_clean, score, _ in match_tuples:
                if threshold <= score < 100:
                    # Find the corresponding original FULLNAME
                    original_matches = roadways_gdf.loc[roadways_gdf['FULLNAME_clean'] == match_clean, 'FULLNAME'].unique()
                    for original_match in original_matches:
                        potential_typos.append({
                            'stop_id': stop_id,
                            'stop_name': stop_name,
                            'street_in_stop_name': street,
                            'similar_road_name_clean': match_clean,         # Normalized road name
                            'similar_road_name_original': original_match,    # Original FULLNAME from shapefile
                            'similarity_score': score
                        })
    return potential_typos

# Initialize list to collect potential typos
potential_typos = []

# Iterate through each stop to find potential typos
for idx, stop in stops_gdf.iterrows():
    stop_id = stop['stop_id']
    stop_name = stop['stop_name']
    stop_streets = extract_street_names(stop_name)
    
    typos = compare_stop_to_roads(
        stop_id,
        stop_name,
        stop_streets,
        road_names_clean,
        roadways_gdf,
        similarity_threshold
    )
    
    potential_typos.extend(typos)

logging.info(f"Total potential typos found before deduplication: {len(potential_typos)}")

# Convert the list of potential typos to a DataFrame
typos_df = pd.DataFrame(potential_typos)

# Sort the DataFrame by similarity score in descending order
typos_df_sorted = typos_df.sort_values(by='similarity_score', ascending=False)

# Remove duplicates
typos_df_sorted.drop_duplicates(inplace=True)

logging.info(f"Total potential typos after deduplication: {typos_df_sorted.shape[0]}")

# Save the results to CSV
if typos_df_sorted.empty:
    logging.info("No potential typos found.")
else:
    typos_df_sorted.to_csv(output_csv_path, index=False)
    logging.info(f"Potential typos saved to {output_csv_path}")



