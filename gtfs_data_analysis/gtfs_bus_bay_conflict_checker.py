#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd

# ================================
# CONFIGURATION SECTION
# ================================
BASE_INPUT_PATH = r"\\folder\path\to\your\gtfs_data"  # Replace with your folder path
BASE_OUTPUT_PATH = r"\\folder\path\to\your\output_folder"  # Replace with your folder path

STOP_TIMES_FILE = "stop_times.txt"
TRIPS_FILE = "trips.txt"
CALENDAR_FILE = "calendar.txt"
STOPS_FILE = "stops.txt"
ROUTES_FILE = "routes.txt"

os.makedirs(BASE_OUTPUT_PATH, exist_ok=True)

# Layover threshold in minutes
LAYOVER_THRESHOLD = 20

# Define service_id for analysis
WHITELISTED_SERVICE_ID = '1'  # Replace with your desired service_id from calendar.txt

# Validate service_id
calendar = pd.read_csv(
    os.path.join(BASE_INPUT_PATH, CALENDAR_FILE), dtype=str
)
available_service_ids = calendar['service_id'].unique()

if WHITELISTED_SERVICE_ID not in available_service_ids:
    raise ValueError(
        f"The service_id '{WHITELISTED_SERVICE_ID}' is invalid.\n"
        f"Available service_id(s) from calendar.txt: "
        f"{', '.join(available_service_ids)}."
    )

# Stops to analyze, based on stop_id
stops_of_interest = ['6307', '6215']

# ================================
# HELPER FUNCTIONS
# ================================
def time_to_seconds(t):
    h, m, s = map(int, t.split(':'))
    return h * 3600 + m * 60 + s

def seconds_to_minute_of_day(sec):
    return (sec % 86400) // 60

def get_trip_ranges_and_ends(block_segments):
    trips_info = []
    for tid in block_segments['trip_id'].unique():
        tsub = block_segments[
            block_segments['trip_id'] == tid
        ].sort_values('arrival_seconds')
        trip_start = tsub['arrival_seconds'].min()
        trip_end = tsub['departure_seconds'].max()
        route_short_name = tsub['route_short_name'].iloc[0]
        direction_id = tsub['direction_id'].iloc[0]
        start_stop = tsub.iloc[0]['stop_id']
        end_stop = tsub.iloc[-1]['stop_id']
        trips_info.append((
            tid, trip_start, trip_end, route_short_name,
            direction_id, start_stop, end_stop
        ))
    trips_info.sort(key=lambda x: x[1])
    return trips_info

def get_minute_status_location(minute, block_segments, LAYOVER_THRESHOLD, trips_info):
    current_sec = minute * 60
    if not trips_info:
        return ("inactive", "inactive", "", "", "")

    earliest_start = min(t[1] for t in trips_info)
    latest_end = max(t[2] for t in trips_info)

    active_trip = None
    for (tid, tstart, tend, rname, dirid, start_stp, end_stp) in trips_info:
        if tstart <= current_sec <= tend:
            active_trip = (
                tid, tstart, tend, rname, dirid, start_stp, end_stp
            )
            break

    if active_trip is not None:
        (
            tid, tstart, tend, rname, dirid, start_stp, end_stp
        ) = active_trip
        tsub = block_segments[
            block_segments['trip_id'] == tid
        ].sort_values('arrival_seconds')

        for i in range(len(tsub)):
            row = tsub.iloc[i]
            arr_sec = row['arrival_seconds'] % 86400
            dep_sec = row['departure_seconds'] % 86400
            nstp = row['next_stop_id']
            narr = row['next_arrival_seconds']
            if pd.notnull(narr):
                narr = narr % 86400

            # Dwelling at stop (inclusive of dep_sec)
            if arr_sec <= current_sec <= dep_sec:
                return (
                    "dwelling at stop", row['stop_id'], rname, 
                    dirid, row['stop_id']
                )

            # Between stops or layover
            if (dep_sec < current_sec and pd.notnull(narr)
                and current_sec < narr):
                if nstp == row['stop_id']:
                    gap = narr - dep_sec
                    if gap > LAYOVER_THRESHOLD * 60:
                        return (
                            "laying over", row['stop_id'], rname, 
                            dirid, row['stop_id']
                        )
                    else:
                        return (
                            "running route", "traveling between stops", 
                            rname, dirid, ""
                        )
                else:
                    return (
                        "running route", "traveling between stops", 
                        rname, dirid, ""
                    )

        # After last departure in the trip range
        return ("inactive", "inactive", "", "", "")

    # Not in active trip
    if current_sec < earliest_start or current_sec > latest_end:
        # Outside all trip windows
        return ("inactive", "inactive", "", "", "")

    # Between trips
    prev_trip = None
    next_trip = None
    for (tid, tstart, tend, rname, dirid, start_stp, end_stp) in trips_info:
        if tend < current_sec:
            prev_trip = (
                tid, tstart, tend, rname, dirid, start_stp, end_stp
            )
        if tstart > current_sec and next_trip is None:
            next_trip = (
                tid, tstart, tend, rname, dirid, start_stp, end_stp
            )
            break

    if prev_trip:
        _, _, _, _, _, _, last_end_stop = prev_trip
        return ("laying over", last_end_stop, "", "", last_end_stop)
    else:
        return ("inactive", "inactive", "", "", "")

# ================================
# DATA LOADING
# ================================
stop_times = pd.read_csv(
    os.path.join(BASE_INPUT_PATH, STOP_TIMES_FILE), dtype=str
)
trips = pd.read_csv(
    os.path.join(BASE_INPUT_PATH, TRIPS_FILE), dtype=str
)
calendar = pd.read_csv(
    os.path.join(BASE_INPUT_PATH, CALENDAR_FILE), dtype=str
)
stops = pd.read_csv(
    os.path.join(BASE_INPUT_PATH, STOPS_FILE), dtype=str
)
routes = pd.read_csv(
    os.path.join(BASE_INPUT_PATH, ROUTES_FILE), dtype=str
)

stop_name_map = stops.set_index('stop_id')['stop_name'].to_dict()

# ================================
# DATA PREPARATION
# ================================
# Whitelist only service_id=1
trips = trips[trips['service_id'] == WHITELISTED_SERVICE_ID]

trips = trips.merge(
    routes[['route_id', 'route_short_name']],
    on='route_id', how='left'
)
stop_times = stop_times[
    stop_times['trip_id'].isin(trips['trip_id'])
]
stop_times = stop_times.merge(
    trips[
        ['trip_id', 'block_id', 'route_id', 'route_short_name', 'direction_id']
    ],
    on='trip_id', how='left'
)

stop_times['arrival_seconds'] = (
    stop_times['arrival_time'].apply(time_to_seconds) % 86400
)
stop_times['departure_seconds'] = (
    stop_times['departure_time'].apply(time_to_seconds) % 86400
)

stop_times.sort_values(['block_id', 'arrival_seconds'], inplace=True)
stop_times['next_stop_id'] = (
    stop_times.groupby('block_id')['stop_id'].shift(-1)
)
stop_times['next_arrival_seconds'] = (
    stop_times.groupby('block_id')['arrival_seconds'].shift(-1)
)
stop_times['next_departure_seconds'] = (
    stop_times.groupby('block_id')['departure_seconds'].shift(-1)
)

stop_times['layover_duration'] = (
    stop_times['next_arrival_seconds'] - stop_times['departure_seconds']
)
stop_times['layover_duration'] = stop_times['layover_duration'].apply(
    lambda x: x + 86400 if (pd.notnull(x) and x < 0) else x
)
stop_times['is_layover'] = (
    (stop_times['stop_id'] == stop_times['next_stop_id']) &
    (stop_times['layover_duration'] <= LAYOVER_THRESHOLD * 60) &
    (stop_times['layover_duration'] > 0)
)

layovers = stop_times[stop_times['is_layover']].copy()
layovers['arrival_seconds'] = layovers['departure_seconds']
layovers['departure_seconds'] = layovers['next_arrival_seconds']
layovers['trip_id'] = layovers['trip_id'] + '_layover'

stop_times = pd.concat([stop_times, layovers], ignore_index=True)
stop_times.sort_values(['block_id', 'arrival_seconds'], inplace=True)

# Filter to blocks that serve the stops_of_interest
blocks_serving_interest = stop_times[
    stop_times['stop_id'].isin(stops_of_interest)
]['block_id'].unique()
filtered_stop_times = stop_times[
    stop_times['block_id'].isin(blocks_serving_interest)
]

all_blocks = filtered_stop_times['block_id'].unique()

print("Generating per-block detailed timeline for blocks that serve stops of interest...")

# Store block DataFrames in memory for later use
block_dataframes = {}

for b_id in all_blocks:
    b_data = filtered_stop_times[
        filtered_stop_times['block_id'] == b_id
    ].copy()
    if b_data.empty:
        continue

    seg_columns = [
        'trip_id', 'route_short_name', 'direction_id', 'stop_id',
        'arrival_seconds', 'departure_seconds', 'next_stop_id',
        'next_arrival_seconds'
    ]
    b_segments = b_data[seg_columns]

    # Get trip ranges
    trips_info = get_trip_ranges_and_ends(b_segments)

    block_df = pd.DataFrame({'minute': range(1440)})
    block_df['time_str'] = block_df['minute'].apply(
        lambda x: f"{x//60:02d}:{x%60:02d}"
    )

    results = []
    for m in block_df['minute']:
        status, location, rname, dirid, sid = get_minute_status_location(
            m, b_segments, LAYOVER_THRESHOLD, trips_info
        )
        results.append((status, location, rname, dirid, sid))

    block_df['status'] = [r[0] for r in results]
    block_df['route_short_name'] = [r[2] for r in results]
    block_df['direction'] = [r[3] for r in results]
    block_df['stop_id'] = [r[4] for r in results]
    block_df['stop_name'] = block_df['stop_id'].apply(
        lambda x: stop_name_map[x] if x in stop_name_map else ""
    )

    block_df['block_id'] = b_id
    inactive_mask = block_df['status'] == "inactive"
    block_df.loc[
        inactive_mask, ['block_id', 'route_short_name',
                       'direction', 'stop_id', 'stop_name']
    ] = ""

    block_dataframes[b_id] = block_df.copy()

    output_file = os.path.join(
        BASE_OUTPUT_PATH, f"block_{b_id}_detailed.xlsx"
    )
    block_df[
        [
            'minute', 'time_str', 'block_id', 'route_short_name',
            'direction', 'stop_id', 'stop_name', 'status'
        ]
    ].to_excel(output_file, index=False)
    print(f"Created {output_file}")

print("Per-block processing completed.")


# ================================
# NEW FUNCTION: PER-STOP EXCELS
# ================================
def create_per_stop_excels(stops_of_interest, block_dataframes, output_folder):
    """
    For each stop in stops_of_interest, create a .xlsx file that includes:
    - A summary sheet listing, for each minute of the day:
      - Which block_ids are present
      - Which route_short_names are present
      - Whether there's a conflict (2+ blocks at the same time)
    - One sheet per block that serves that stop.
    """

    for s_id in stops_of_interest:
        # Find which blocks serve this stop
        blocks_serving_stop = []
        for b_id, bdf in block_dataframes.items():
            if (bdf['stop_id'] == s_id).any():
                blocks_serving_stop.append(b_id)

        if not blocks_serving_stop:
            continue

        summary_df = pd.DataFrame({'minute': range(1440)})
        summary_df['time_str'] = summary_df['minute'].apply(
            lambda x: f"{x//60:02d}:{x%60:02d}"
        )
        summary_df['blocks_present'] = [[] for _ in range(1440)]
        summary_df['routes_present'] = [[] for _ in range(1440)]

        for b_id in blocks_serving_stop:
            bdf = block_dataframes[b_id]
            presence_mask = (
                (bdf['stop_id'] == s_id) &
                (bdf['status'] != 'inactive')
            )
            present_minutes = bdf[presence_mask]

            for idx, row in present_minutes.iterrows():
                m = row['minute']
                if row['block_id']:
                    summary_df.at[m, 'blocks_present'].append(row['block_id'])
                if row['route_short_name']:
                    summary_df.at[m, 'routes_present'].append(row['route_short_name'])

        summary_df['blocks_present_str'] = summary_df['blocks_present'].apply(
            lambda x: ", ".join(sorted(set(x)))
        )
        summary_df['routes_present_str'] = summary_df['routes_present'].apply(
            lambda x: ", ".join(sorted(set(x)))
        )
        summary_df['num_blocks'] = summary_df['blocks_present'].apply(
            lambda x: len(set(x))
        )
        summary_df['conflict'] = summary_df['num_blocks'].apply(
            lambda x: "Yes" if x > 1 else "No"
        )

        stop_name = stop_name_map.get(s_id, "")
        safe_stop_name = "".join(
            [c if c.isalnum() else "_" for c in stop_name]
        ) or "UnknownStop"
        output_file = os.path.join(
            output_folder, f"stop_{s_id}_{safe_stop_name}_blocks.xlsx"
        )

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            summary_cols = [
                'minute', 'time_str', 'num_blocks', 
                'blocks_present_str', 'routes_present_str', 'conflict'
            ]
            summary_df[summary_cols].to_excel(
                writer, sheet_name='Summary', index=False
            )

            for b_id in blocks_serving_stop:
                bdf = block_dataframes[b_id]
                bdf[
                    [
                        'minute', 'time_str', 'block_id', 
                        'route_short_name', 'direction', 'stop_id', 
                        'stop_name', 'status'
                    ]
                ].to_excel(
                    writer, sheet_name=f"Block_{b_id}", index=False
                )

        print(f"Created per-stop file for stop {s_id}: {output_file}")


create_per_stop_excels(stops_of_interest, block_dataframes, BASE_OUTPUT_PATH)

print("Per-stop processing completed.")


# ================================
# NEW FUNCTION: SUMMARY OF SUMMARIES
# ================================
def create_summary_of_summaries(stops_of_interest, block_dataframes, output_folder):
    """
    Create a 'summary of summaries' file that shows, for the entire cluster of stops:
    - How many buses are present in each minute of the day
    - Which routes and blocks are present (aggregated across all stops)
    - Additionally, for each filtered stop of interest:
      - Add columns listing which routes and blocks are present at that specific stop.

    The 'conflict' column is removed, and 'num_blocks' remains and now reflects
    the total number of unique blocks present at all filtered stops combined.
    """
    # Initialize a global summary DataFrame
    cluster_summary_df = pd.DataFrame({'minute': range(1440)})
    cluster_summary_df['time_str'] = cluster_summary_df['minute'].apply(
        lambda x: f"{x//60:02d}:{x%60:02d}"
    )
    cluster_summary_df['blocks_present'] = [[] for _ in range(1440)]
    cluster_summary_df['routes_present'] = [[] for _ in range(1440)]

    # For each stop, also create columns to store per-stop block/route presence
    per_stop_blocks = {}
    per_stop_routes = {}
    for s_id in stops_of_interest:
        # Safe stop name
        stop_name = stop_name_map.get(s_id, "UnknownStop")
        safe_stop_name = "".join(
            [c if c.isalnum() else "_" for c in stop_name]
        ) or "UnknownStop"
        # Initialize per-stop lists
        per_stop_blocks[s_id] = [[] for _ in range(1440)]
        per_stop_routes[s_id] = [[] for _ in range(1440)]
        # Add placeholder columns to cluster_summary_df (will fill later)
        cluster_summary_df[
            f"{safe_stop_name}_{s_id}_blocks_present_str"
        ] = ""
        cluster_summary_df[
            f"{safe_stop_name}_{s_id}_routes_present_str"
        ] = ""

    # Aggregate presence data across all stops
    for s_id in stops_of_interest:
        # Identify blocks serving this stop
        blocks_serving_stop = [
            b_id for b_id, bdf in block_dataframes.items()
            if (bdf['stop_id'] == s_id).any()
        ]

        for b_id in blocks_serving_stop:
            bdf = block_dataframes[b_id]
            presence_mask = (
                (bdf['stop_id'] == s_id) &
                (bdf['status'] != 'inactive')
            )
            present_minutes = bdf[presence_mask]

            for idx, row in present_minutes.iterrows():
                minute = row['minute']
                # Add block and route info to the cluster-level summary for all stops
                if row['block_id']:
                    cluster_summary_df.at[m, 'blocks_present'].append(row['block_id'])
                    per_stop_blocks[s_id][m].append(row['block_id'])
                if row['route_short_name']:
                    cluster_summary_df.at[m, 'routes_present'].append(
                        row['route_short_name']
                    )
                    per_stop_routes[s_id][m].append(
                        row['route_short_name']
                    )

    # Convert the top-level lists into strings
    cluster_summary_df['blocks_present_str'] = cluster_summary_df['blocks_present'].apply(
        lambda x: ", ".join(sorted(set(x)))
    )
    cluster_summary_df['routes_present_str'] = cluster_summary_df['routes_present'].apply(
        lambda x: ", ".join(sorted(set(x)))
    )

    # Populate the per-stop columns now that all data is aggregated
    for s_id in stops_of_interest:
        stop_name = stop_name_map.get(s_id, "UnknownStop")
        safe_stop_name = "".join(
            [c if c.isalnum() else "_" for c in stop_name]
        ) or "UnknownStop"
        cluster_summary_df[
            f"{safe_stop_name}_{s_id}_blocks_present_str"
        ] = [
            ", ".join(sorted(set(x))) for x in per_stop_blocks[s_id]
        ]
        cluster_summary_df[
            f"{safe_stop_name}_{s_id}_routes_present_str"
        ] = [
            ", ".join(sorted(set(x))) for x in per_stop_routes[s_id]
        ]

    # Finally, compute num_blocks as the total number of unique blocks from all filtered stops
    cluster_summary_df['num_blocks'] = cluster_summary_df['blocks_present'].apply(
        lambda x: len(set(x))
    )

    # Write out the cluster-level summary
    output_file = os.path.join(
        output_folder, "cluster_summary_of_summaries.xlsx"
    )
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Define columns explicitly without 'conflict'
        base_cols = [
            'minute', 'time_str', 'num_blocks', 
            'blocks_present_str', 'routes_present_str'
        ]
        per_stop_cols = []
        for s_id in stops_of_interest:
            stop_name = stop_name_map.get(s_id, "UnknownStop")
            safe_stop_name = "".join(
                [c if c.isalnum() else "_" for c in stop_name]
            ) or "UnknownStop"
            per_stop_cols.extend([
                f"{safe_stop_name}_{s_id}_blocks_present_str",
                f"{safe_stop_name}_{s_id}_routes_present_str"
            ])

        final_cols = base_cols + per_stop_cols
        cluster_summary_df[final_cols].to_excel(
            writer, sheet_name='Cluster_Summary', index=False
        )

    print(f"Created cluster-level summary of summaries: {output_file}")


# Create the summary of summaries
create_summary_of_summaries(
    stops_of_interest, block_dataframes, BASE_OUTPUT_PATH
)
