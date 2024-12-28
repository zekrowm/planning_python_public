"""
This module processes GTFS data along with a demographics shapefile
to compute coverage areas for specific routes.
"""

import os
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# GTFS data path
GTFS_DATA_PATH = r"C:\Folder\Path\To\Your\GTFS_data" # Replace with your folder path

# List of route_short_name values to process
# Replace with your exact route short name(s) to process
ROUTES = [
    "101", "152", "340", "630", "642", "651", "698", "798",
    "983", "RIBS1", "RIBS2", "RIBS3", "RIBS4", "RIBS5"
]

# Buffer distance in miles
BUFFER_DISTANCE = 0.5

# Demographics shapefile path
DEMOGRAPHICS_SHP_PATH = r"C:\File\Path\To\Your\census_blocks.shp" # Replace with your file path

# Synthetic fields to process
# Replace with your service population data column names
SYNTHETIC_FIELDS = [
    "total_pop", "total_hh", "tot_empl", "low_wage", "mid_wage", "high_wage",
    "est_minori", "est_lep", "est_lo_veh", "est_lo_v_1", "est_youth", "est_elderl"
]

# CRS - set the coordinate reference system for area calculations
# Replace with the appropriate EPSG code for your region
CRS_EPSG_CODE = 3395

# Output directory - set the directory for saving the final processed data
OUTPUT_DIRECTORY = r"C:\Folder\Path\To\Your\Output" # Replace with your folder path

# ==============================
# END OF CONFIGURATION SECTION
# ==============================

try:
    # Check if GTFS files exist before loading
    gtfs_files = ["trips.txt", "stop_times.txt", "routes.txt", "stops.txt", "calendar.txt"]
    for gtfs_file in gtfs_files:
        file_path = os.path.join(GTFS_DATA_PATH, gtfs_file)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(
                f"File not found: {file_path}"
            )

    if not os.path.isfile(DEMOGRAPHICS_SHP_PATH):
        raise FileNotFoundError(
            f"File not found: {DEMOGRAPHICS_SHP_PATH}"
        )

    # Load GTFS files
    trips = pd.read_csv(os.path.join(GTFS_DATA_PATH, "trips.txt"))
    stop_times = pd.read_csv(os.path.join(GTFS_DATA_PATH, "stop_times.txt"))
    routes_df = pd.read_csv(os.path.join(GTFS_DATA_PATH, "routes.txt"))
    stops_df = pd.read_csv(os.path.join(GTFS_DATA_PATH, "stops.txt"))
    calendar = pd.read_csv(os.path.join(GTFS_DATA_PATH, "calendar.txt"))

    # Filter for services that are available on weekdays
    relevant_service_ids = calendar[
        (calendar['monday'] == 1) & (calendar['tuesday'] == 1) &
        (calendar['wednesday'] == 1) & (calendar['thursday'] == 1) &
        (calendar['friday'] == 1)
    ]['service_id']

    # Filter trips to include only those that match the relevant service IDs
    trips = trips[trips['service_id'].isin(relevant_service_ids)]

    # Get all available route_short_names
    all_routes = routes_df['route_short_name'].unique()

    # Filter routes for the specified routes
    included_routes_df = routes_df[routes_df['route_short_name'].isin(ROUTES)]

    # Print the routes included in the analysis with single quotes
    included_routes_list = sorted(included_routes_df['route_short_name'].unique())
    included_routes_with_quotes = [f"'{route}'" for route in included_routes_list]
    included_routes_str = ', '.join(included_routes_with_quotes)
    print(f"Routes included in analysis: {included_routes_str}")

    # Determine and print the routes excluded from the analysis
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

    # Merge with stops to get stop information
    merged_data = pd.merge(merged_data, stops_df, on='stop_id')

    # Create GeoDataFrame of stops with route information
    merged_data['geometry'] = merged_data.apply(
        lambda row: Point(row['stop_lon'], row['stop_lat']),
        axis=1
    )
    stops_gdf = gpd.GeoDataFrame(merged_data, geometry='geometry', crs="EPSG:4326")

    # Reproject data to the specified CRS
    stops_gdf = stops_gdf.to_crs(epsg=CRS_EPSG_CODE)
    demographics_gdf = gpd.read_file(DEMOGRAPHICS_SHP_PATH)
    demographics_gdf = demographics_gdf.to_crs(epsg=CRS_EPSG_CODE)

    # Buffer stops
    buffer_distance_meters = BUFFER_DISTANCE * 1609.34  # Convert miles to meters
    stops_gdf['geometry'] = stops_gdf.geometry.buffer(buffer_distance_meters)

    # Drop duplicates to avoid overlapping buffers
    stops_gdf = stops_gdf[['route_short_name', 'stop_id', 'geometry']].drop_duplicates()

    # Dissolve buffers by route
    dissolved_buffers_gdf = stops_gdf.dissolve(by='route_short_name').reset_index()

    # Process each route separately
    for route in ROUTES:
        print(f"\nProcessing route: {route}")

        try:
            # Get the dissolved buffer for the route
            route_buffer_gdf = dissolved_buffers_gdf[
                dissolved_buffers_gdf['route_short_name'] == route
            ]

            # Clip demographic data to the buffer
            clipped_demographics_gdf = gpd.clip(demographics_gdf, route_buffer_gdf)

            # Add and calculate original area field in acres
            clipped_demographics_gdf['area_ac_og'] = (
                clipped_demographics_gdf.geometry.area / 4046.86
            )

            # Add and calculate clipped area field
            clipped_demographics_gdf['area_ac_cl'] = (
                clipped_demographics_gdf.geometry.area / 4046.86
            )

            # Calculate percentage of original area
            clipped_demographics_gdf['area_perc'] = (
                clipped_demographics_gdf['area_ac_cl'] /
                clipped_demographics_gdf['area_ac_og']
            )

            # Add and calculate synthetic data fields using existing column names
            for original_field in SYNTHETIC_FIELDS:
                # Convert fields to numeric, replacing non-numeric values with 0
                clipped_demographics_gdf[original_field] = pd.to_numeric(
                    clipped_demographics_gdf[original_field],
                    errors='coerce'
                ).fillna(0)
                clipped_demographics_gdf[f"synthetic_{original_field}"] = (
                    clipped_demographics_gdf['area_perc'] *
                    clipped_demographics_gdf[original_field]
                )

            # Summarize and print results
            totals = clipped_demographics_gdf[
                [f"synthetic_{field}" for field in SYNTHETIC_FIELDS]
            ].sum().round(0)

            for field, value in totals.items():
                field_display = field.replace('synthetic_', '').replace('_', ' ').title()
                print(
                    f"Total Synthetic {field_display} for route {route}: {int(value)}"
                )

            # Export the final shapefile for each route
            os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
            final_output_path = os.path.join(
                OUTPUT_DIRECTORY,
                f"{route}_service_buffer_data.shp"
            )
            clipped_demographics_gdf.to_file(final_output_path)
            print(
                f"Final shapefile for route {route} exported successfully to "
                f"{final_output_path}"
            )

            # Plot for visual check
            fig, ax = plt.subplots(figsize=(10, 10))
            ax.set_xlim(route_buffer_gdf.total_bounds[[0, 2]])
            ax.set_ylim(route_buffer_gdf.total_bounds[[1, 3]])

            demographics_gdf.boundary.plot(
                ax=ax, color='black', linewidth=0.5, label='Demographics Boundary'
            )
            route_buffer_gdf.plot(
                ax=ax, color='blue', alpha=0.5, label='Route Buffer'
            )
            route_stops_gdf = stops_gdf[
                stops_gdf['route_short_name'] == route
            ]
            route_stops_gdf.plot(
                ax=ax, color='red', markersize=10, label='Route Stops'
            )

            plt.title(f"Route {route} Buffer and Demographics Overlay")
            plt.legend()
            plt.show()

        except FileNotFoundError as err:
            print(f"File not found while processing route {route}: {err}")
        except ValueError as err:
            print(f"ValueError while processing route {route}: {err}")
        except Exception as err:  # pylint: disable=broad-exception-caught
            print(f"An unexpected error occurred while processing route {route}: {err}")

except FileNotFoundError as main_err:
    print(f"File not found: {main_err}")
except Exception as main_err:  # pylint: disable=broad-exception-caught
    print(f"An error occurred: {main_err}")
