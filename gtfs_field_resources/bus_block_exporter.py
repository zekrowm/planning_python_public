"""
GTFS Block Timeline Generator

This script processes General Transit Feed Specification (GTFS) data to create detailed
minute-by-minute timelines for each transit block. It performs the following steps:

1. **Data Loading**: Imports GTFS files such as stop_times, trips, calendar, stops, and routes.
2. **Data Preparation**: Applies optional filters for service IDs and route short names, converts
   time fields to seconds, calculates layover durations, and organizes data for analysis.
3. **Timeline Generation**: Generates Excel spreadsheets for each block, detailing the block's
   status (e.g., running, dwelling, laying over) at each minute. If specified, it also organizes
   these spreadsheets into folders for stops of interest.

**Configuration**:
- **Input/Output Paths**: Set the directories for input GTFS files and output spreadsheets.
- **Filters**: Optionally filter data by service IDs, route short names, and specific stops.
- **Thresholds**: Define layover duration thresholds to identify layover periods.

**Usage**:
Ensure the GTFS files are placed in the specified input directory and adjust the configuration
parameters as needed. Run the script to generate detailed block timelines in the output directory.
"""

import math
import os
import shutil

import pandas as pd


# ================================
# CONFIGURATION
# ================================
BASE_INPUT_PATH = (
    r"C:\Path\To\Your\Input"
    r"\Folder"
)  # Replace with your folder path
BASE_OUTPUT_PATH = (
    r"C:\Path\To\Your\Output"
    r"\Folder"
)  # Replace with your folder path

STOP_TIMES_FILE = "stop_times.txt"
TRIPS_FILE = "trips.txt"
CALENDAR_FILE = "calendar.txt"
STOPS_FILE = "stops.txt"
ROUTES_FILE = "routes.txt"

LAYOVER_THRESHOLD = 20  # minutes

# -- Optional Filters --
# Leave these lists empty ([]) to include all possible values for that dimension.
FILTER_SERVICE_IDS = ['1']  # e.g. ['1', '2'] or []
FILTER_ROUTE_SHORT_NAMES = []  # e.g. ['101', '202'] or []
STOPS_OF_INTEREST = ['1001', '1002']  # e.g. ['1001', '1002'] or []


# ================================
# HELPER FUNCTIONS
# ================================
def time_to_seconds(time_str):
    """Convert HH:MM:SS time format to total seconds."""
    hours, minutes, seconds = map(int, time_str.split(':'))
    return hours * 3600 + minutes * 60 + seconds


def get_trip_ranges_and_ends(block_segments):
    """Extract trip start/end times and metadata for each trip in a block."""
    trips_info = []
    for trip_id in block_segments['trip_id'].unique():
        trip_subset = block_segments[block_segments['trip_id'] == trip_id].sort_values(
            'arrival_seconds'
        )
        trip_start = trip_subset['arrival_seconds'].min()
        trip_end = trip_subset['departure_seconds'].max()
        route_short_name = trip_subset['route_short_name'].iloc[0]
        direction_id = trip_subset['direction_id'].iloc[0]
        start_stop = trip_subset.iloc[0]['stop_id']
        end_stop = trip_subset.iloc[-1]['stop_id']
        trips_info.append(
            (
                trip_id,
                trip_start,
                trip_end,
                route_short_name,
                direction_id,
                start_stop,
                end_stop,
            )
        )
    trips_info.sort(key=lambda x: x[1])
    return trips_info


def get_minute_status_location(
    minute, block_segments, layover_threshold, trips_info
):
    """
    Determine the block's status at a specific minute
    (dwelling, running, laying over, or inactive).
    """
    current_sec = minute * 60
    if not trips_info:
        return "inactive", "inactive", "", "", ""

    earliest_start = min(trip[1] for trip in trips_info)
    latest_end = max(trip[2] for trip in trips_info)

    # Check if we’re within any trip's start/end
    active_trip = None
    for trip in trips_info:
        trip_id, t_start, t_end, route_name, direction_id, start_stop, end_stop = trip
        if t_start <= current_sec <= t_end:
            active_trip = trip
            break

    if active_trip is not None:
        (
            trip_id,
            t_start,
            t_end,
            route_name,
            direction_id,
            start_stop,
            end_stop,
        ) = active_trip
        trip_subset = block_segments[
            block_segments['trip_id'] == trip_id
        ].sort_values('arrival_seconds')

        for _, row in trip_subset.iterrows():
            arr_sec = row['arrival_seconds']
            dep_sec = row['departure_seconds']
            next_stop_id = row['next_stop_id']
            next_arr_sec = row['next_arrival_seconds']

            # Dwelling at stop
            if arr_sec <= current_sec <= dep_sec:
                return (
                    "dwelling at stop",
                    row['stop_id'],
                    route_name,
                    direction_id,
                    row['stop_id'],
                )

            # Between stops or layover
            if pd.notnull(next_arr_sec) and dep_sec < current_sec < next_arr_sec:
                if next_stop_id == row['stop_id']:
                    gap = next_arr_sec - dep_sec
                    if gap > layover_threshold * 60:
                        return (
                            "laying over",
                            row['stop_id'],
                            route_name,
                            direction_id,
                            row['stop_id'],
                        )
                    else:
                        return (
                            "running route",
                            "traveling between stops",
                            route_name,
                            direction_id,
                            "",
                        )
                else:
                    return (
                        "running route",
                        "traveling between stops",
                        route_name,
                        direction_id,
                        "",
                    )

        # After last departure in the trip range
        return "inactive", "inactive", "", "", ""

    # Outside or between trips
    if current_sec < earliest_start or current_sec > latest_end:
        return "inactive", "inactive", "", "", ""

    # Between trips: consider last trip end stop
    prev_trip = None
    next_trip = None
    for trip in trips_info:
        trip_id, t_start, t_end, route_name, direction_id, start_stop, end_stop = trip
        if t_end < current_sec:
            prev_trip = trip
        if t_start > current_sec and next_trip is None:
            next_trip = trip
            break

    if prev_trip:
        _, _, _, _, _, _, last_end_stop = prev_trip
        return "laying over", last_end_stop, "", "", last_end_stop

    return "inactive", "inactive", "", "", ""


def load_data(
    base_input_path,
    stop_times_file,
    trips_file,
    calendar_file,
    stops_file,
    routes_file,
):
    """Load GTFS files into DataFrames."""
    calendar_path = os.path.join(base_input_path, calendar_file)
    trips_path = os.path.join(base_input_path, trips_file)
    stop_times_path = os.path.join(base_input_path, stop_times_file)
    stops_path = os.path.join(base_input_path, stops_file)
    routes_path = os.path.join(base_input_path, routes_file)

    calendar_df = pd.read_csv(calendar_path, dtype=str)
    trips_df = pd.read_csv(trips_path, dtype=str)
    stop_times_df = pd.read_csv(stop_times_path, dtype=str)
    stops_df = pd.read_csv(stops_path, dtype=str)
    routes_df = pd.read_csv(routes_path, dtype=str)

    return calendar_df, trips_df, stop_times_df, stops_df, routes_df


def prepare_data(
    calendar_df,
    trips_df,
    stop_times_df,
    stops_df,
    routes_df,
    service_ids_filter,
    route_short_names_filter,
    layover_threshold,
):
    """
    Optionally filter data by service_id and/or route_short_name.
    Then merge relevant columns, calculate times in seconds,
    determine next stops, handle layovers, etc.

    Returns (stop_times, stop_name_map, minute_range).
    """

    # 1) Filter by service_id (calendar) if user specified any
    if service_ids_filter:
        # Validate the user’s requested service IDs
        all_service_ids = set(calendar_df['service_id'].unique())
        invalid_requested = set(service_ids_filter) - all_service_ids
        if invalid_requested:
            raise ValueError(
                "Invalid service_id(s): "
                f"{','.join(invalid_requested)}.\n"
                f"Available: {','.join(all_service_ids)}"
            )
        trips_df = trips_df[trips_df['service_id'].isin(service_ids_filter)]

    # 2) Merge route info onto trips
    trips_df = trips_df.merge(
        routes_df[['route_id', 'route_short_name']],
        on='route_id',
        how='left',
    )

    # 3) Filter by route_short_name if user specified any
    if route_short_names_filter:
        # Validate the user’s requested route_short_names
        all_route_names = set(routes_df['route_short_name'].unique())
        invalid_requested = set(route_short_names_filter) - all_route_names
        if invalid_requested:
            raise ValueError(
                "Invalid route_short_name(s): "
                f"{','.join(invalid_requested)}.\n"
                f"Available: {','.join(all_route_names)}"
            )
        trips_df = trips_df[trips_df['route_short_name'].isin(route_short_names_filter)]

    # 4) Filter stop_times down to remaining trip_ids
    stop_times_df = stop_times_df[stop_times_df['trip_id'].isin(trips_df['trip_id'])]

    # 5) Merge trip info needed for analysis
    stop_times_df = stop_times_df.merge(
        trips_df[
            ['trip_id', 'block_id', 'route_id', 'route_short_name', 'direction_id']
        ],
        on='trip_id',
        how='left',
    )

    # 6) Convert times to seconds
    stop_times_df['arrival_seconds'] = stop_times_df['arrival_time'].apply(
        time_to_seconds
    )
    stop_times_df['departure_seconds'] = stop_times_df['departure_time'].apply(
        time_to_seconds
    )

    # 7) Determine maximum departure_seconds to set the minute range
    max_departure_seconds = stop_times_df['departure_seconds'].max()
    max_minute = int(math.ceil(max_departure_seconds / 60)) if pd.notnull(max_departure_seconds) else 0
    minute_range = range(0, max_minute + 1)

    # 8) Sort and compute next-stop columns
    stop_times_df.sort_values(['block_id', 'arrival_seconds'], inplace=True)
    stop_times_df['next_stop_id'] = stop_times_df.groupby('block_id')[
        'stop_id'
    ].shift(-1)
    stop_times_df['next_arrival_seconds'] = stop_times_df.groupby(
        'block_id'
    )['arrival_seconds'].shift(-1)
    stop_times_df['next_departure_seconds'] = stop_times_df.groupby(
        'block_id'
    )['departure_seconds'].shift(-1)

    # 9) Compute layovers
    stop_times_df['layover_duration'] = (
        stop_times_df['next_arrival_seconds'] - stop_times_df['departure_seconds']
    )
    stop_times_df['layover_duration'] = stop_times_df['layover_duration'].apply(
        lambda x: x + 86400 if (pd.notnull(x) and x < 0) else x
    )
    stop_times_df['is_layover'] = (
        (stop_times_df['stop_id'] == stop_times_df['next_stop_id'])
        & (stop_times_df['layover_duration'] <= layover_threshold * 60)
        & (stop_times_df['layover_duration'] > 0)
    )

    # Duplicate layover rows
    layovers = stop_times_df[stop_times_df['is_layover']].copy()
    layovers['arrival_seconds'] = layovers['departure_seconds']
    layovers['departure_seconds'] = layovers['next_arrival_seconds']
    layovers['trip_id'] = layovers['trip_id'] + '_layover'

    # Append layovers and re-sort
    stop_times_df = pd.concat([stop_times_df, layovers], ignore_index=True)
    stop_times_df.sort_values(['block_id', 'arrival_seconds'], inplace=True)

    # 10) Create stop_name map
    stop_name_map = stops_df.set_index('stop_id')['stop_name'].to_dict()

    return stop_times_df, stop_name_map, minute_range


def generate_block_spreadsheets(
    stop_times,
    stop_name_map,
    minute_range,
    layover_threshold,
    stops_of_interest,
    output_folder,
):
    """
    Create one spreadsheet per block. Copy them into each stop folder
    if that block serves the stop.
    """
    all_blocks = stop_times['block_id'].dropna().unique()
    print(f"Generating per-block detailed timeline for {len(all_blocks)} blocks...")

    # If we have stops_of_interest, create subfolders for each
    if stops_of_interest:
        for stop_id in stops_of_interest:
            os.makedirs(os.path.join(output_folder, f"stop_{stop_id}"), exist_ok=True)

    for block_id in all_blocks:
        block_data = stop_times[stop_times['block_id'] == block_id].copy()
        if block_data.empty:
            continue

        segment_columns = [
            'trip_id',
            'route_short_name',
            'direction_id',
            'stop_id',
            'arrival_seconds',
            'departure_seconds',
            'next_stop_id',
            'next_arrival_seconds',
        ]
        block_segments = block_data[segment_columns]

        # Get trip ranges
        trips_info = get_trip_ranges_and_ends(block_segments)

        # Build a minute-by-minute DataFrame
        block_df = pd.DataFrame({'minute': minute_range})
        block_df['time_str'] = block_df['minute'].apply(
            lambda x: f"{x // 60:02d}:{x % 60:02d}"
        )

        results = []
        for minute in block_df['minute']:
            status, location, route_name, direction_id, stop_id = get_minute_status_location(
                minute, block_segments, layover_threshold, trips_info
            )
            results.append((status, location, route_name, direction_id, stop_id))

        block_df['status'] = [result[0] for result in results]
        block_df['route_short_name'] = [result[2] for result in results]
        block_df['direction'] = [result[3] for result in results]
        block_df['stop_id'] = [result[4] for result in results]
        block_df['stop_name'] = block_df['stop_id'].apply(
            lambda x: stop_name_map.get(x, "")
        )

        block_df['block_id'] = block_id
        inactive_mask = block_df['status'] == "inactive"
        block_df.loc[
            inactive_mask,
            ['block_id', 'route_short_name', 'direction', 'stop_id', 'stop_name'],
        ] = ""

        # Write Excel file
        output_file = os.path.join(output_folder, f"block_{block_id}_detailed.xlsx")
        block_df[
            [
                'minute',
                'time_str',
                'block_id',
                'route_short_name',
                'direction',
                'stop_id',
                'stop_name',
                'status',
            ]
        ].to_excel(output_file, index=False)

        # Copy the file to each stop folder if block serves that stop
        if stops_of_interest:
            stops_in_block = set(block_df['stop_id'].unique())
            relevant_stops = set(stops_of_interest).intersection(stops_in_block)
            for stop_id in relevant_stops:
                stop_folder = os.path.join(output_folder, f"stop_{stop_id}")
                dest_file = os.path.join(
                    stop_folder, f"block_{block_id}_detailed.xlsx"
                )
                shutil.copyfile(output_file, dest_file)

        print(f"Created: {output_file}")

    print("Per-block processing completed.")


# ================================
# MAIN EXECUTION
# ================================
def main():
    os.makedirs(BASE_OUTPUT_PATH, exist_ok=True)

    # 1) LOAD DATA
    (
        calendar_df,
        trips_df,
        stop_times_df,
        stops_df,
        routes_df,
    ) = load_data(
        BASE_INPUT_PATH,
        STOP_TIMES_FILE,
        TRIPS_FILE,
        CALENDAR_FILE,
        STOPS_FILE,
        ROUTES_FILE,
    )

    # 2) PREPARE DATA (with optional filters)
    stop_times, stop_name_map, minute_range = prepare_data(
        calendar_df,
        trips_df,
        stop_times_df,
        stops_df,
        routes_df,
        FILTER_SERVICE_IDS,
        FILTER_ROUTE_SHORT_NAMES,
        LAYOVER_THRESHOLD,
    )

    # 3) (Optional) Print Unique Block IDs
    unique_block_ids = stop_times['block_id'].dropna().unique()
    print("\n=== Unique Block IDs ===")
    for block_id in unique_block_ids:
        print(block_id)
    print(f"\nTotal number of unique block IDs: {len(unique_block_ids)}\n")

    # 4) GENERATE BLOCK SPREADSHEETS
    generate_block_spreadsheets(
        stop_times,
        stop_name_map,
        minute_range,
        LAYOVER_THRESHOLD,
        STOPS_OF_INTEREST,
        BASE_OUTPUT_PATH,
    )

    print("All done.")


if __name__ == "__main__":
    main()
