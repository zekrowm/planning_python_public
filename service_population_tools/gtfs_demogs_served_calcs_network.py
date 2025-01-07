"""
This module processes GTFS data and a demographic shapefile to produce
buffers around transit stops and compute synthetic demographic measures.
"""

import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# pylint: disable=invalid-name
GTFS_DATA_PATH = r"\\your_file_path\here\\"

# List of short_short_name values to process
ROUTES = ["101", "102", "103"]  # Replace with your own values

# Buffer distance in miles
# TODO: Add feature to set multiple route lists with different buffer distances
BUFFER_DISTANCE = 0.5

# Demographics shapefile path
DEMOGRAPHICS_SHP_PATH = r"C:\path\to\census_blocks.shp"

# Synthetic fields to process
SYNTHETIC_FIELDS = [
    "total_pop", "total_hh", "tot_empl", "low_wage", "mid_wage", "high_wage",
    "est_minori", "est_lep", "est_lo_veh", "est_lo_v_1", "est_youth", "est_elderl",
    "est_low_in"
]  # Replace with your own values

# CRS - set the coordinate reference system for area calculations
# Replace with the appropriate EPSG code for your region
CRS_EPSG_CODE = 3395

# Output directory - set the directory for saving the final processed data
OUTPUT_DIRECTORY = r"C:\path\to\output"

# ==============================
# END OF CONFIGURATION SECTION
# ==============================

try:
    # Check if GTFS files exist before loading
    gtfs_files = ["trips.txt", "stop_times.txt", "routes.txt", "stops.txt", "calendar.txt"]
    for gtfs_file in gtfs_files:
        file_path = os.path.join(GTFS_DATA_PATH, gtfs_file)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    if not os.path.isfile(DEMOGRAPHICS_SHP_PATH):
        raise FileNotFoundError(f"File not found: {DEMOGRAPHICS_SHP_PATH}")

    # Load GTFS files
    trips = pd.read_csv(os.path.join(GTFS_DATA_PATH, "trips.txt"))
    stop_times = pd.read_csv(os.path.join(GTFS_DATA_PATH, "stop_times.txt"))
    routes_df = pd.read_csv(os.path.join(GTFS_DATA_PATH, "routes.txt"))
    stops_df = pd.read_csv(os.path.join(GTFS_DATA_PATH, "stops.txt"))
    calendar = pd.read_csv(os.path.join(GTFS_DATA_PATH, "calendar.txt"))

    # Filter for services that are available on weekdays
    relevant_service_ids = calendar[
        (calendar['monday'] == 1)
        & (calendar['tuesday'] == 1)
        & (calendar['wednesday'] == 1)
        & (calendar['thursday'] == 1)
        & (calendar['friday'] == 1)
    ]['service_id']

    # Filter trips to include only those that match the relevant service IDs
    trips = trips[trips['service_id'].isin(relevant_service_ids)]

    # Get all available route_short_names
    all_routes = routes_df['route_short_name'].unique()

    # Filter routes for the specified routes
    included_routes_df = routes_df[routes_df['route_short_name'].isin(ROUTES)]

    # **Print the routes included in the analysis with single quotes**
    included_routes_list = sorted(included_routes_df['route_short_name'].unique())
    included_routes_with_quotes = [f"'{route}'" for route in included_routes_list]
    included_routes_str = ', '.join(included_routes_with_quotes)
    print(f"Routes included in analysis: {included_routes_str}")

    # **Determine and print the routes excluded from the analysis**
    excluded_routes_set = set(all_routes) - set(included_routes_list)
    excluded_routes_list = sorted(excluded_routes_set)
    excluded_routes_with_quotes = [f"'{route}'" for route in excluded_routes_list]
    excluded_routes_str = ', '.join(excluded_routes_with_quotes)
    print(f"Routes excluded from analysis: {excluded_routes_str}")

    # Proceed with merging trips and routes
    trips = pd.merge(
        trips,
        included_routes_df[['route_id', 'route_short_name']],
        on='route_id'
    )

    # Merge trips with stop_times
    merged_data = pd.merge(stop_times, trips, on='trip_id')

    # Get unique stop_ids from merged_data
    stop_ids = merged_data['stop_id'].unique()

    # Get the stops corresponding to those stop_ids
    stops_filtered = stops_df[stops_df['stop_id'].isin(stop_ids)]

    # Create GeoDataFrame of stops
    stops_filtered['geometry'] = stops_filtered.apply(
        lambda row: Point(row['stop_lon'], row['stop_lat']), axis=1
    )
    stops_gdf = gpd.GeoDataFrame(stops_filtered, geometry='geometry', crs="EPSG:4326")

    # Reproject data to the specified CRS
    stops_gdf = stops_gdf.to_crs(epsg=CRS_EPSG_CODE)
    demographics_gdf = gpd.read_file(DEMOGRAPHICS_SHP_PATH)
    demographics_gdf = demographics_gdf.to_crs(epsg=CRS_EPSG_CODE)

    # Buffer stops
    buffer_distance_meters = BUFFER_DISTANCE * 1609.34  # Convert miles to meters
    stops_gdf['geometry'] = stops_gdf.buffer(buffer_distance_meters)

    # Dissolve buffers
    dissolved_buffers_gdf = stops_gdf.dissolve().reset_index(drop=True)

    # Plot for visual check
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.set_xlim(stops_gdf.total_bounds[[0, 2]])
    ax.set_ylim(stops_gdf.total_bounds[[1, 3]])

    demographics_gdf.boundary.plot(
        ax=ax, color='black', linewidth=0.5, label='Demographics Boundary'
    )
    dissolved_buffers_gdf.plot(
        ax=ax, color='blue', alpha=0.5, label='Dissolved Buffer'
    )
    stops_gdf.plot(
        ax=ax, color='red', markersize=10, label='Route Stops'
    )

    plt.title("Combined Route Buffer and Demographics Overlay")
    plt.legend()
    plt.show()

    # Add and calculate original area field in acres
    demographics_gdf['area_ac_og'] = demographics_gdf.geometry.area / 4046.86

    # Clip demographic data to dissolved buffer
    clipped_demographics_gdf = gpd.clip(demographics_gdf, dissolved_buffers_gdf)

    # Add and calculate clipped area field
    clipped_demographics_gdf['area_ac_cl'] = (
        clipped_demographics_gdf.geometry.area / 4046.86
    )

    # Calculate percentage of original area
    clipped_demographics_gdf['area_perc'] = (
        clipped_demographics_gdf['area_ac_cl'] / clipped_demographics_gdf['area_ac_og']
    )

    # Add and calculate synthetic data fields using existing column names
    for original_field in SYNTHETIC_FIELDS:
        # Convert fields to numeric, replacing non-numeric values with 0
        clipped_demographics_gdf[original_field] = pd.to_numeric(
            clipped_demographics_gdf[original_field],
            errors='coerce'
        ).fillna(0)
        clipped_demographics_gdf[f"synthetic_{original_field}"] = (
            clipped_demographics_gdf['area_perc'] * clipped_demographics_gdf[original_field]
        )

    # Summarize and print results for all routes combined
    synthetic_cols = [f"synthetic_{field}" for field in SYNTHETIC_FIELDS]
    totals = clipped_demographics_gdf[synthetic_cols].sum().round(0)
    for field, value in totals.items():
        title_case_field = field.replace('_', ' ').title()
        print(f"Total Synthetic {title_case_field} for all routes: {int(value)}")

    # Export the final shapefile for all routes combined
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    final_output_path = os.path.join(
        OUTPUT_DIRECTORY, "all_routes_service_buffer_data.shp"
    )
    clipped_demographics_gdf.to_file(final_output_path)
    print(f"Final shapefile for all routes exported successfully to {final_output_path}")

# If you know which exceptions to catch, replace `Exception` with those.
except FileNotFoundError as e:
    print(f"A file was not found: {e}")
except Exception as e:  # pylint: disable=broad-except
    print(f"An error occurred: {e}")
