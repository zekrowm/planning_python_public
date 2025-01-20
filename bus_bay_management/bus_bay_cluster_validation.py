"""
GTFS Bus Bay Cluster Validation

This module validates GTFS bus arrival data by checking clusters of bus stops for consistency.
Planners can use this to check proposed field check bus stop clusters or bus bay conflict check clusters.
It identifies potential errors like similar stop names, nearby excluded stops, distant included stops, 
and stops with different names.
Results are exported as Excel files and shapefiles for further analysis and visual inspection.
"""

import os

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from rapidfuzz import fuzz, process

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# Input directory for GTFS files
BASE_INPUT_PATH = r'\\your_input_path\here\\'

# Output directory for results
BASE_OUTPUT_PATH = r'\\your_output_path\here\\'

# Define clusters with provided stop IDs
# Format: {'Cluster Name': [stop_id1, stop_id2, ...]}
# You can leave the clusters dictionary empty if you're not ready to define clusters.
# The script will still run and export all stops for visual inspection.
clusters = {
    # 'Downtown Bus Station': [1, 2, 3],  # Replace with your actual cluster names and stop IDs
    # 'Airport Terminal': [4, 5, 6],
    # Add more clusters as needed
}

# Thresholds for analysis
SIMILARITY_THRESHOLD = 85           # For finding similar stop names (0-100)
DISTANCE_THRESHOLD_NEARBY = 200     # Distance in feet for nearby stops (default changed to feet)
DISTANCE_THRESHOLD_DISTANT = 1000   # Distance in feet for distant included stops (default changed to feet)
SIMILARITY_THRESHOLD_NAMES = 70     # For included stops with different names (0-100)

# CRS for distance calculations (Projected CRS)
# NOTE:
#  - Choose a CRS that is appropriate for your city or region to ensure accurate distance calculations.
#  - For example:
#  - Avoid using Web Mercator (EPSG:3857) for distance calculations, as it distorts distances at certain latitudes.
#  - The default CRS below is set to NAD83 (EPSG:2248) in feet, suitable for the DC region.

DISTANCE_CRS_EPSG = 2248  # NAD83 / Metro DC area (feet)

# ==============================
# END OF CONFIGURATION SECTION
# ==============================

# Create output directory if it doesn't exist
if not os.path.exists(BASE_OUTPUT_PATH):
    os.makedirs(BASE_OUTPUT_PATH)

# Load GTFS stops.txt file
stops_file = os.path.join(BASE_INPUT_PATH, 'stops.txt')
if not os.path.exists(stops_file):
    raise FileNotFoundError(f"stops.txt not found in {BASE_INPUT_PATH}")

stops = pd.read_csv(stops_file)

# Convert stop_id to string
stops['stop_id'] = stops['stop_id'].astype(str)

# Convert stops to GeoDataFrame
stops['geometry'] = stops.apply(
    lambda row: Point(row['stop_lon'], row['stop_lat']), axis=1
)
stops_gdf = gpd.GeoDataFrame(stops, geometry='geometry', crs='EPSG:4326')

# Reproject to a projected CRS for accurate distance calculations
stops_gdf = stops_gdf.to_crs(epsg=DISTANCE_CRS_EPSG)

# Initialize clusters if not provided
if not clusters:
    print("No clusters defined. Proceeding without cluster analysis.")
    # Create empty GeoDataFrames for included and excluded stops
    included_stops = gpd.GeoDataFrame(columns=stops_gdf.columns)
    excluded_stops = stops_gdf.copy()
else:
    # Convert cluster stop IDs to strings
    clusters = {
        cluster_name: [str(stop_id) for stop_id in stop_ids]
        for cluster_name, stop_ids in clusters.items()
    }

    # Create a list of included stop IDs
    included_stop_ids = [
        stop_id for ids in clusters.values() for stop_id in ids
    ]
    included_stops = stops_gdf[stops_gdf['stop_id'].isin(included_stop_ids)].copy()
    included_stops['cluster'] = None

    # Assign cluster names to included stops
    for cluster_name, stop_ids in clusters.items():
        included_stops.loc[
            included_stops['stop_id'].isin(stop_ids), 'cluster'
        ] = cluster_name

    # Create a GeoDataFrame for excluded stops
    excluded_stops = stops_gdf[~stops_gdf['stop_id'].isin(included_stop_ids)].copy()

    # Reset index of excluded_stops to ensure idx matches the DataFrame rows
    excluded_stops = excluded_stops.reset_index(drop=True)

# Functions for analysis
def find_similar_stop_names(included_stops, excluded_stops, threshold=SIMILARITY_THRESHOLD):
    if included_stops.empty or excluded_stops.empty:
        return pd.DataFrame(
            columns=[
                'included_stop_name', 'excluded_stop_name', 'similarity_score',
                'stop_id', 'stop_lat', 'stop_lon'
            ]
        )
    # Convert stop names to lowercase for case-insensitive comparison
    included_stops['stop_name_lower'] = included_stops['stop_name'].str.lower()
    excluded_stops['stop_name_lower'] = excluded_stops['stop_name'].str.lower()

    similar_stops = []
    included_names = included_stops['stop_name_lower'].unique()
    for name in included_names:
        matches = process.extract(
            name,
            excluded_stops['stop_name_lower'],
            scorer=fuzz.token_sort_ratio,
            limit=None
        )
        for match_name, score, idx in matches:
            if score >= threshold:
                similar_stop = excluded_stops.iloc[idx]
                original_included_name = included_stops[
                    included_stops['stop_name_lower'] == name
                ]['stop_name'].iloc[0]
                similar_stops.append({
                    'included_stop_name': original_included_name,
                    'excluded_stop_name': similar_stop['stop_name'],
                    'similarity_score': score,
                    'stop_id': similar_stop['stop_id'],
                    'stop_lat': similar_stop['stop_lat'],
                    'stop_lon': similar_stop['stop_lon']
                })
    # Define the columns to ensure they are present even if the DataFrame is empty
    columns = [
        'included_stop_name', 'excluded_stop_name', 'similarity_score',
        'stop_id', 'stop_lat', 'stop_lon'
    ]
    return pd.DataFrame(similar_stops, columns=columns)


def find_nearby_excluded_stops(included_stops, excluded_stops, distance_threshold=DISTANCE_THRESHOLD_NEARBY):
    if included_stops.empty or excluded_stops.empty:
        return pd.DataFrame(
            columns=[
                'included_stop_id', 'included_stop_name',
                'excluded_stop_id', 'excluded_stop_name', 'distance_m'
            ]
        )
    nearby_stops = []
    for _, included_stop in included_stops.iterrows():
        # Buffer the included stop by the distance threshold
        buffer = included_stop.geometry.buffer(distance_threshold)
        # Find excluded stops within the buffer
        nearby = excluded_stops[excluded_stops.geometry.within(buffer)]
        for _, nearby_stop in nearby.iterrows():
            distance = included_stop.geometry.distance(nearby_stop.geometry)
            nearby_stops.append({
                'included_stop_id': included_stop['stop_id'],
                'included_stop_name': included_stop['stop_name'],
                'excluded_stop_id': nearby_stop['stop_id'],
                'excluded_stop_name': nearby_stop['stop_name'],
                'distance_m': distance
            })
    # Define the columns to ensure they are present even if the DataFrame is empty
    columns = [
        'included_stop_id', 'included_stop_name',
        'excluded_stop_id', 'excluded_stop_name', 'distance_m'
    ]
    return pd.DataFrame(nearby_stops, columns=columns)


def find_distant_included_stops(included_stops, distance_threshold=DISTANCE_THRESHOLD_DISTANT):
    if included_stops.empty:
        return pd.DataFrame(
            columns=[
                'stop_id', 'stop_name', 'cluster', 'min_distance_to_cluster_m'
            ]
        )
    distant_stops = []
    clusters_list = included_stops['cluster'].unique()
    for cluster in clusters_list:
        cluster_stops = included_stops[included_stops['cluster'] == cluster]
        for _, stop in cluster_stops.iterrows():
            # Calculate distances to other stops in the cluster
            other_stops = cluster_stops.drop(stop.name)
            if other_stops.empty:
                continue
            distances = other_stops.geometry.distance(stop.geometry)
            # If the minimum distance is greater than the threshold, record the stop
            if distances.min() > distance_threshold:
                distant_stops.append({
                    'stop_id': stop['stop_id'],
                    'stop_name': stop['stop_name'],
                    'cluster': cluster,
                    'min_distance_to_cluster_m': distances.min()
                })
    # Define the columns to ensure they are present even if the DataFrame is empty
    columns = [
        'stop_id', 'stop_name', 'cluster', 'min_distance_to_cluster_m'
    ]
    return pd.DataFrame(distant_stops, columns=columns)


def find_different_named_included_stops(included_stops, similarity_threshold=SIMILARITY_THRESHOLD_NAMES):
    if included_stops.empty:
        return pd.DataFrame(
            columns=[
                'stop_id', 'stop_name', 'cluster', 'max_similarity_score'
            ]
        )
    different_named_stops = []
    clusters_list = included_stops['cluster'].unique()
    for cluster in clusters_list:
        cluster_stops = included_stops[included_stops['cluster'] == cluster]
        names = cluster_stops['stop_name'].unique()
        for _, stop in cluster_stops.iterrows():
            similarities = [
                fuzz.token_sort_ratio(stop['stop_name'], name)
                for name in names if name != stop['stop_name']
            ]
            if similarities and max(similarities) < similarity_threshold:
                different_named_stops.append({
                    'stop_id': stop['stop_id'],
                    'stop_name': stop['stop_name'],
                    'cluster': cluster,
                    'max_similarity_score': max(similarities)
                })
    # Define the columns to ensure they are present even if the DataFrame is empty
    columns = [
        'stop_id', 'stop_name', 'cluster', 'max_similarity_score'
    ]
    return pd.DataFrame(different_named_stops, columns=columns)


# Perform analysis only if clusters are defined
if clusters:
    # Find excluded stops with similar names
    similar_name_stops = find_similar_stop_names(
        included_stops, excluded_stops, threshold=SIMILARITY_THRESHOLD
    )

    # Print the number of outputs for similar name stops
    print(f"Number of similar name stops found: {len(similar_name_stops)}")

    # Find excluded stops that are physically close to included stops
    nearby_excluded_stops = find_nearby_excluded_stops(
        included_stops, excluded_stops, distance_threshold=DISTANCE_THRESHOLD_NEARBY
    )

    # Print the number of outputs for nearby excluded stops
    print(f"Number of nearby excluded stops found: {len(nearby_excluded_stops)}")

    # Find included stops that are distant from their cluster
    distant_included_stops = find_distant_included_stops(
        included_stops, distance_threshold=DISTANCE_THRESHOLD_DISTANT
    )

    # Print the number of outputs for distant included stops
    print(f"Number of distant included stops found: {len(distant_included_stops)}")

    # Find included stops with different names
    different_named_included_stops = find_different_named_included_stops(
        included_stops, similarity_threshold=SIMILARITY_THRESHOLD_NAMES
    )

    # Print the number of outputs for different named included stops
    print(f"Number of different named included stops found: {len(different_named_included_stops)}")
else:
    # If clusters are not defined, create empty DataFrames for outputs
    similar_name_stops = pd.DataFrame()
    nearby_excluded_stops = pd.DataFrame()
    distant_included_stops = pd.DataFrame()
    different_named_included_stops = pd.DataFrame()

# Output the results
output_directory = os.path.join(BASE_OUTPUT_PATH, 'stops_check')
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Save the results to Excel files, ensuring headers are written even if DataFrame is empty
def save_to_excel(df, filename):
    file_path = os.path.join(output_directory, filename)
    # Ensure the DataFrame has the correct columns
    if df.empty:
        # Create an empty DataFrame with the correct columns based on filename
        if 'similar_names' in filename:
            columns = [
                'included_stop_name', 'excluded_stop_name',
                'similarity_score', 'stop_id', 'stop_lat', 'stop_lon'
            ]
        elif 'nearby' in filename:
            columns = [
                'included_stop_id', 'included_stop_name',
                'excluded_stop_id', 'excluded_stop_name', 'distance_m'
            ]
        elif 'distant' in filename:
            columns = [
                'stop_id', 'stop_name', 'cluster',
                'min_distance_to_cluster_m'
            ]
        elif 'different_names' in filename:
            columns = [
                'stop_id', 'stop_name', 'cluster',
                'max_similarity_score'
            ]
        else:
            columns = df.columns
        df = pd.DataFrame(columns=columns)
    # Write to Excel with headers
    df.to_excel(file_path, index=False, header=True)

# Save the DataFrames
save_to_excel(similar_name_stops, 'excluded_stops_similar_names.xlsx')
save_to_excel(nearby_excluded_stops, 'excluded_stops_nearby.xlsx')
save_to_excel(distant_included_stops, 'included_stops_distant.xlsx')
save_to_excel(different_named_included_stops, 'included_stops_different_names.xlsx')

# Export included and excluded stops as shapefiles for visual inspection
included_stops_shp = os.path.join(output_directory, 'included_stops.shp')
excluded_stops_shp = os.path.join(output_directory, 'excluded_stops.shp')
all_stops_shp = os.path.join(output_directory, 'all_stops.shp')

# Save shapefiles, ensuring empty GeoDataFrames are handled
if not included_stops.empty:
    included_stops.to_file(included_stops_shp)
else:
    print("No included stops to export.")

if not excluded_stops.empty:
    excluded_stops.to_file(excluded_stops_shp)
else:
    print("No excluded stops to export.")

# Export all stops
stops_gdf.to_file(all_stops_shp)

print("Stops check completed. Results have been saved to the 'stops_check' directory.")
