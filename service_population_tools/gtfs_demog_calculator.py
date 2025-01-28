"""
Combined GTFS and Demographics Analysis Script with INCLUSION/EXCLUSION Filters

This script processes GTFS data and a demographic shapefile to produce
buffers around transit stops and compute estimated demographic measures.

It supports two modes:
1) "network": Dissolves buffers for all (final) included routes combined.
2) "route": Performs a separate buffer-and-clip analysis per route.

Additionally, it supports two route-filter lists:
- ROUTES_TO_INCLUDE: If non-empty, only these routes are considered.
- ROUTES_TO_EXCLUDE: If non-empty, these routes are removed from consideration.

If both lists are empty, all routes are analyzed.

Usage:
    - Adjust the CONFIGURATION variables below.
    - Run the script (e.g., `python combined_analysis.py`).
    - Check console output, shapefile exports, and optional plots.
"""

import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# =============================================================================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# =============================================================================

# Select analysis mode: "network" or "route"
ANALYSIS_MODE = "network"

# Paths
GTFS_DATA_PATH = r"C:\Path\To\GTFS_data_folder"
DEMOGRAPHICS_SHP_PATH = r"C:\Path\To\census_blocks.shp"
OUTPUT_DIRECTORY = r"C:\Path\To\Output"

# Route filters:
# 1) ROUTES_TO_INCLUDE: If non-empty, only these routes are considered.
# 2) ROUTES_TO_EXCLUDE: If non-empty, these routes are removed.
# If both are empty, all routes in routes.txt are used.
ROUTES_TO_INCLUDE = ["101", "102"]  # e.g. [] for no include filter
ROUTES_TO_EXCLUDE = ["104"]         # e.g. [] for no exclude filter

# Buffer distance in miles
BUFFER_DISTANCE = 0.25

# Optional FIPS filter (list of codes). Empty list = no filter.
FIPS_FILTER = ["11001"]

# Fields in demographics shapefile to multiply by area ratio
SYNTHETIC_FIELDS = [
    "total_pop", "total_hh", "tot_empl", "low_wage", "mid_wage", "high_wage",
    "est_minori", "est_lep", "est_lo_veh", "est_lo_v_1", "est_youth",
    "est_elderl", "est_low_in"
]

# EPSG code for projected coordinate system used in area calculations
CRS_EPSG_CODE = 3395 # Replace with EPSG for your study area

# GTFS files expected
REQUIRED_GTFS_FILES = [
    "trips.txt", "stop_times.txt", "routes.txt", "stops.txt", "calendar.txt"
]

# =============================================================================
# END OF CONFIGURATION SECTION
# =============================================================================


def load_gtfs_data(gtfs_path: str) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """
    Load required GTFS files into DataFrames. Raises FileNotFoundError if missing.

    :param gtfs_path: Path to the folder containing GTFS .txt files.
    :return: (trips, stop_times, routes_df, stops_df, calendar) DataFrames.
    """
    for filename in REQUIRED_GTFS_FILES:
        if not os.path.isfile(os.path.join(gtfs_path, filename)):
            raise FileNotFoundError(f"Missing file: {filename} in {gtfs_path}")

    trips = pd.read_csv(os.path.join(gtfs_path, "trips.txt"))
    stop_times = pd.read_csv(os.path.join(gtfs_path, "stop_times.txt"))
    routes_df = pd.read_csv(os.path.join(gtfs_path, "routes.txt"))
    stops_df = pd.read_csv(os.path.join(gtfs_path, "stops.txt"))
    calendar = pd.read_csv(os.path.join(gtfs_path, "calendar.txt"))

    return trips, stop_times, routes_df, stops_df, calendar


def filter_weekday_service(calendar_df: pd.DataFrame) -> pd.Series:
    """
    Return service_ids for routes that run Monday through Friday.

    :param calendar_df: DataFrame from calendar.txt.
    :return: Series of service_id values available on all weekdays.
    """
    weekday_filter = (
        (calendar_df["monday"] == 1)
        & (calendar_df["tuesday"] == 1)
        & (calendar_df["wednesday"] == 1)
        & (calendar_df["thursday"] == 1)
        & (calendar_df["friday"] == 1)
    )
    return calendar_df[weekday_filter]["service_id"]


def apply_fips_filter(
    demog_gdf: gpd.GeoDataFrame, fips_filter: list[str]
) -> gpd.GeoDataFrame:
    """
    Filter a demographics GeoDataFrame by a list of FIPS codes (optional).

    :param demog_gdf: A GeoDataFrame of demographic data with column 'FIPS'.
    :param fips_filter: List of FIPS codes to keep. If empty, no filter is applied.
    :return: Filtered or unfiltered GeoDataFrame.
    """
    if fips_filter:
        before_count = len(demog_gdf)
        demog_gdf = demog_gdf[demog_gdf["FIPS"].isin(fips_filter)]
        after_count = len(demog_gdf)
        print(
            f"Applied FIPS filter: {fips_filter} "
            f"(reduced from {before_count} to {after_count} records)"
        )
    else:
        print("No FIPS filter applied; processing all FIPS codes.")
    return demog_gdf


def get_included_routes(
    routes_df: pd.DataFrame,
    routes_to_include: list[str],
    routes_to_exclude: list[str]
) -> pd.DataFrame:
    """
    Determine which routes to keep by applying inclusion/exclusion lists.

    1) Start with all routes in routes_df.
    2) If routes_to_include is non-empty, keep only those in that list.
    3) If routes_to_exclude is non-empty, remove those from the result.

    :param routes_df: DataFrame from routes.txt.
    :param routes_to_include: List of route_short_names to include.
    :param routes_to_exclude: List of route_short_names to exclude.
    :return: DataFrame containing only the final included routes.
    """
    filtered = routes_df.copy()

    if routes_to_include:
        filtered = filtered[filtered["route_short_name"].isin(routes_to_include)]

    if routes_to_exclude:
        filtered = filtered[
            ~filtered["route_short_name"].isin(routes_to_exclude)
        ]

    final_count = len(filtered)
    print(f"Including {final_count} routes after apply include/exclude lists.")
    included_names = ", ".join(sorted(filtered["route_short_name"].unique()))
    if included_names:
        print(f"  Included Routes: {included_names}")
    else:
        print("  Included Routes: None")
    return filtered


def clip_and_calculate_synthetic_fields(
    demographics_gdf: gpd.GeoDataFrame,
    buffer_gdf: gpd.GeoDataFrame,
    synthetic_fields: list[str]
) -> gpd.GeoDataFrame:
    """
    Clip demographics_gdf with the buffer geometry and calculate synthetic fields.
    Correctly computes the area percentage based on original polygon areas.
    """

    # Step 1: Ensure we have an "original area" column
    if "area_ac_og" not in demographics_gdf.columns:
        demographics_gdf["area_ac_og"] = demographics_gdf.geometry.area / 4046.86  # Convert to acres

    # Step 2: Clip the demographics GeoDataFrame with the buffer GeoDataFrame
    clipped_gdf = gpd.clip(demographics_gdf, buffer_gdf)

    # Step 3: Compute clipped area and area percentage
    clipped_gdf["area_ac_cl"] = clipped_gdf.geometry.area / 4046.86  # Clipped area in acres
    clipped_gdf["area_perc"] = clipped_gdf["area_ac_cl"] / clipped_gdf["area_ac_og"]

    # Handle cases where original area is zero to avoid division by zero
    clipped_gdf["area_perc"].replace([float('inf'), -float('inf')], 0, inplace=True)
    clipped_gdf["area_perc"].fillna(0, inplace=True)

    # Step 4: Apply partial weighting to synthetic fields
    for field in synthetic_fields:
        # Ensure the field is numeric; non-numeric values are set to 0
        clipped_gdf[field] = pd.to_numeric(clipped_gdf[field], errors="coerce").fillna(0)
        # Calculate synthetic field based on area percentage
        clipped_gdf[f"synthetic_{field}"] = clipped_gdf["area_perc"] * clipped_gdf[field]

    return clipped_gdf


def do_network_analysis(
    trips: pd.DataFrame,
    stop_times: pd.DataFrame,
    routes_df: pd.DataFrame,
    stops_df: pd.DataFrame,
    demographics_gdf: gpd.GeoDataFrame,
    routes_to_include: list[str],
    routes_to_exclude: list[str],
    buffer_distance_mi: float,
    output_dir: str,
    synthetic_fields: list[str]
) -> None:
    """
    Perform a single "network-wide" buffer analysis across the final included routes.

    :param trips: DataFrame from trips.txt (filtered to relevant service IDs).
    :param stop_times: DataFrame from stop_times.txt.
    :param routes_df: DataFrame from routes.txt.
    :param stops_df: DataFrame from stops.txt.
    :param demographics_gdf: GeoDataFrame of demographic data (projected & FIPS-filtered).
    :param routes_to_include: List of route_short_names to include (if any).
    :param routes_to_exclude: List of route_short_names to exclude (if any).
    :param buffer_distance_mi: Buffer distance in miles.
    :param output_dir: Destination folder for output shapefile.
    :param synthetic_fields: Columns to compute synthetic values for.
    """
    print("\n=== Network-wide Analysis ===")

    final_routes_df = get_included_routes(
        routes_df, routes_to_include, routes_to_exclude
    )
    if final_routes_df.empty:
        print("No routes remain after filters. Aborting network analysis.")
        return

    trips_merged = pd.merge(
        trips,
        final_routes_df[["route_id", "route_short_name"]],
        on="route_id"
    )
    merged_data = pd.merge(stop_times, trips_merged, on="trip_id")
    merged_data = pd.merge(merged_data, stops_df, on="stop_id")

    merged_data["geometry"] = merged_data.apply(
        lambda row: Point(row["stop_lon"], row["stop_lat"]), axis=1
    )
    stops_gdf = gpd.GeoDataFrame(
        merged_data, geometry="geometry", crs="EPSG:4326"
    ).to_crs(epsg=CRS_EPSG_CODE)

    buffer_distance_meters = buffer_distance_mi * 1609.34
    stops_gdf["geometry"] = stops_gdf.buffer(buffer_distance_meters)

    network_buffer_gdf = stops_gdf.dissolve().reset_index(drop=True)
    clipped_result = clip_and_calculate_synthetic_fields(
        demographics_gdf, network_buffer_gdf, synthetic_fields
    )

    synthetic_cols = [f"synthetic_{fld}" for fld in synthetic_fields]
    totals = clipped_result[synthetic_cols].sum().round(0)

    print("Network-wide totals:")
    for col, value in totals.items():
        display_col = col.replace("synthetic_", "").replace("_", " ").title()
        print(f"  Total Synthetic {display_col}: {int(value)}")

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(
        output_dir, "all_routes_service_buffer_data.shp"
    )
    clipped_result.to_file(out_path)
    print(f"Exported network shapefile: {out_path}")

    fig, ax = plt.subplots(figsize=(10, 10))
    network_buffer_gdf.plot(ax=ax, color="blue", alpha=0.5, label="Network Buffer")
    stops_gdf.boundary.plot(ax=ax, color="red", linewidth=0.5, label="Stop Buffers")
    plt.title("Network Buffer")
    plt.legend()
    plt.show()


def do_route_by_route_analysis(
    trips: pd.DataFrame,
    stop_times: pd.DataFrame,
    routes_df: pd.DataFrame,
    stops_df: pd.DataFrame,
    demographics_gdf: gpd.GeoDataFrame,
    routes_to_include: list[str],
    routes_to_exclude: list[str],
    buffer_distance_mi: float,
    output_dir: str,
    synthetic_fields: list[str]
) -> None:
    """
    Perform a buffer/clip analysis separately for each route in the final route set.

    :param trips: DataFrame from trips.txt (filtered to relevant service IDs).
    :param stop_times: DataFrame from stop_times.txt.
    :param routes_df: DataFrame from routes.txt.
    :param stops_df: DataFrame from stops.txt.
    :param demographics_gdf: GeoDataFrame of demographic data (projected & FIPS-filtered).
    :param routes_to_include: List of route_short_names to include (if any).
    :param routes_to_exclude: List of route_short_names to exclude (if any).
    :param buffer_distance_mi: Buffer distance in miles.
    :param output_dir: Destination folder for output shapefiles.
    :param synthetic_fields: Columns to compute synthetic values for.
    """
    print("\n=== Route-by-Route Analysis ===")

    final_routes_df = get_included_routes(
        routes_df, routes_to_include, routes_to_exclude
    )
    if final_routes_df.empty:
        print("No routes remain after filters. Aborting route-by-route analysis.")
        return

    trips_merged = pd.merge(
        trips,
        final_routes_df[["route_id", "route_short_name"]],
        on="route_id"
    )
    merged_data = pd.merge(stop_times, trips_merged, on="trip_id")
    merged_data = pd.merge(merged_data, stops_df, on="stop_id")

    merged_data["geometry"] = merged_data.apply(
        lambda row: Point(row["stop_lon"], row["stop_lat"]), axis=1
    )
    stops_gdf = gpd.GeoDataFrame(
        merged_data, geometry="geometry", crs="EPSG:4326"
    ).to_crs(epsg=CRS_EPSG_CODE)

    buffer_distance_meters = buffer_distance_mi * 1609.34
    stops_gdf["geometry"] = stops_gdf.geometry.buffer(buffer_distance_meters)
    stops_gdf = stops_gdf[["route_short_name", "stop_id", "geometry"]].drop_duplicates()

    dissolved_by_route_gdf = stops_gdf.dissolve(
        by="route_short_name"
    ).reset_index()
    unique_route_names = dissolved_by_route_gdf["route_short_name"].unique()

    for route_name in unique_route_names:
        print(f"\nProcessing route: {route_name}")
        route_buffer_gdf = dissolved_by_route_gdf[
            dissolved_by_route_gdf["route_short_name"] == route_name
        ]
        if route_buffer_gdf.empty:
            print(f"No stops found for route '{route_name}' - skipping.")
            continue

        clipped_result = clip_and_calculate_synthetic_fields(
            demographics_gdf, route_buffer_gdf, synthetic_fields
        )

        synthetic_cols = [f"synthetic_{f}" for f in synthetic_fields]
        totals = clipped_result[synthetic_cols].sum().round(0)
        for col, val in totals.items():
            display_col = col.replace("synthetic_", "").replace("_", " ").title()
            print(f"  Total Synthetic {display_col} for route {route_name}: {int(val)}")

        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(
            output_dir, f"{route_name}_service_buffer_data.shp"
        )
        clipped_result.to_file(out_path)
        print(f"Exported shapefile for route {route_name}: {out_path}")

        fig, ax = plt.subplots(figsize=(10, 10))
        route_buffer_gdf.plot(
            ax=ax, color="blue", alpha=0.5, label=f"Route {route_name} Buffer"
        )
        plt.title(f"Route {route_name} Buffer Overlay")
        plt.legend()
        plt.show()


def main():
    """
    Main driver function. Adjust ANALYSIS_MODE and route filter variables
    (ROUTES_TO_INCLUDE, ROUTES_TO_EXCLUDE) in the configuration section.
    """
    try:
        trips, stop_times, routes_df, stops_df, calendar = load_gtfs_data(
            GTFS_DATA_PATH
        )
        relevant_service_ids = filter_weekday_service(calendar)
        trips = trips[trips["service_id"].isin(relevant_service_ids)]

        if not os.path.isfile(DEMOGRAPHICS_SHP_PATH):
            raise FileNotFoundError(
                f"Demographics shapefile not found: {DEMOGRAPHICS_SHP_PATH}"
            )
        demographics_gdf = gpd.read_file(DEMOGRAPHICS_SHP_PATH)
        demographics_gdf = apply_fips_filter(demographics_gdf, FIPS_FILTER)
        demographics_gdf = demographics_gdf.to_crs(epsg=CRS_EPSG_CODE)

        if ANALYSIS_MODE.lower() == "network":
            do_network_analysis(
                trips, stop_times, routes_df, stops_df,
                demographics_gdf, ROUTES_TO_INCLUDE, ROUTES_TO_EXCLUDE,
                BUFFER_DISTANCE, OUTPUT_DIRECTORY, SYNTHETIC_FIELDS
            )
        elif ANALYSIS_MODE.lower() == "route":
            do_route_by_route_analysis(
                trips, stop_times, routes_df, stops_df,
                demographics_gdf, ROUTES_TO_INCLUDE, ROUTES_TO_EXCLUDE,
                BUFFER_DISTANCE, OUTPUT_DIRECTORY, SYNTHETIC_FIELDS
            )
        else:
            raise ValueError(f"Invalid ANALYSIS_MODE: {ANALYSIS_MODE}")

        print("\nAnalysis completed successfully.")

    except FileNotFoundError as fnf_err:
        print(f"File not found error: {fnf_err}")
    except Exception as err:
        print(f"Unexpected error occurred: {err}")


if __name__ == "__main__":
    main()
