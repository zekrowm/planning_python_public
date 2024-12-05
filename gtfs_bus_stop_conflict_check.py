#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import timedelta
import os

# ================================
# CONFIGURATION SECTION
# ================================

# Input and output folder paths
base_input_path = r"C:\Path\To\Your\GTFS\Data"
base_output_path = r"C:\Path\To\Your\Output\Folder"

# Input file names
stop_times_file = "stop_times.txt"
trips_file = "trips.txt"
calendar_file = "calendar.txt"
stops_file = "stops.txt"

# Ensure output directory exists
os.makedirs(base_output_path, exist_ok=True)

# Option to process all stops or a specific list
process_all_stops = True  # Set to False to process specific stops

# List of stop_ids to process (used if process_all_stops is False)
stop_ids_to_process = ['2832', '1097', '1098']  # Make sure stop IDs are strings

# Layover threshold in minutes
layover_threshold = 20

# Service days to consider (modify as needed)
service_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']

# ================================
# END CONFIGURATION SECTION
# ================================

# ================================
# DATA LOADING
# ================================

# Load GTFS data
stop_times = pd.read_csv(os.path.join(base_input_path, stop_times_file), dtype=str)
trips = pd.read_csv(os.path.join(base_input_path, trips_file), dtype=str)
calendar = pd.read_csv(os.path.join(base_input_path, calendar_file), dtype=str)
stops = pd.read_csv(os.path.join(base_input_path, stops_file), dtype=str)

# Convert times to proper format
def time_to_seconds(t):
    h, m, s = map(int, t.split(':'))
    return h * 3600 + m * 60 + s

# ================================
# DATA PREPARATION
# ================================

# Filter calendar for active service_ids on the specified service days
active_services = calendar[calendar[service_days].apply(lambda x: x == '1').any(axis=1)]['service_id'].unique()
trips = trips[trips['service_id'].isin(active_services)]

# Filter stop_times for the trips with active service_ids
stop_times = stop_times[stop_times['trip_id'].isin(trips['trip_id'])]

# Merge stop_times with trips to get block_id and route_id
stop_times = stop_times.merge(trips[['trip_id', 'block_id', 'route_id']], on='trip_id', how='left')

# If not processing all stops, filter stop_times for the specified stop_ids
if not process_all_stops:
    stop_times = stop_times[stop_times['stop_id'].isin(stop_ids_to_process)]

# Convert arrival_time and departure_time to seconds
stop_times['arrival_seconds'] = stop_times['arrival_time'].apply(time_to_seconds)
stop_times['departure_seconds'] = stop_times['departure_time'].apply(time_to_seconds)

# Correct times that go over midnight
stop_times['arrival_seconds'] = stop_times['arrival_seconds'] % 86400
stop_times['departure_seconds'] = stop_times['departure_seconds'] % 86400

# ================================
# HANDLE LAYOVERS
# ================================

# Sort stop_times for layover detection
stop_times.sort_values(['block_id', 'arrival_seconds'], inplace=True)

# Identify layovers at the same stop within the threshold
stop_times['next_stop_id'] = stop_times.groupby('block_id')['stop_id'].shift(-1)
stop_times['next_arrival_seconds'] = stop_times.groupby('block_id')['arrival_seconds'].shift(-1)
stop_times['next_departure_seconds'] = stop_times.groupby('block_id')['departure_seconds'].shift(-1)
stop_times['layover_duration'] = stop_times['next_arrival_seconds'] - stop_times['departure_seconds']

# Correct negative durations due to overnight trips
stop_times['layover_duration'] = stop_times['layover_duration'].apply(lambda x: x + 86400 if x < 0 else x)

# Identify layovers that start at the same stop within the threshold
stop_times['is_layover'] = (
    (stop_times['stop_id'] == stop_times['next_stop_id']) &
    (stop_times['layover_duration'] <= layover_threshold * 60) &
    (stop_times['layover_duration'] > 0)
)

# Create layover records
layovers = stop_times[stop_times['is_layover']].copy()
layovers['arrival_seconds'] = layovers['departure_seconds']
layovers['departure_seconds'] = layovers['next_arrival_seconds']
layovers['trip_id'] = layovers['trip_id'] + '_layover'
layovers['route_id'] = layovers['route_id']  # Route remains the same during layover

# Combine original stop_times with layover times
stop_times = pd.concat([stop_times, layovers], ignore_index=True)

# ================================
# CONFLICT DETECTION WITHOUT PER-MINUTE EXPANSION
# ================================

print("Detecting conflicts at stops...")

# Function to detect conflicts at a stop
def detect_conflicts(stop_df):
    # Sort intervals by arrival time
    intervals = stop_df[['arrival_seconds', 'departure_seconds', 'block_id']].sort_values('arrival_seconds').values
    conflicts = False
    last_departure = -1
    for arrival, departure, block_id in intervals:
        if arrival < last_departure:
            conflicts = True
            break
        last_departure = max(last_departure, departure)
    return conflicts

# Get list of stops to process
if process_all_stops:
    stop_ids_to_process = stop_times['stop_id'].unique()

# Dictionary to hold stops with conflicts
stops_with_conflicts = []

# Detect conflicts at each stop
for stop_id in stop_ids_to_process:
    stop_df = stop_times[stop_times['stop_id'] == stop_id]
    # Group by block_id and get intervals
    intervals = stop_df[['arrival_seconds', 'departure_seconds', 'block_id']]
    # Check for overlaps
    intervals = intervals.sort_values('arrival_seconds')
    intervals_list = intervals.values.tolist()
    conflicts = False
    active_blocks = []
    for arrival, departure, block_id in intervals_list:
        # Remove blocks that have departed
        active_blocks = [blk for blk in active_blocks if blk[1] > arrival]
        if active_blocks:
            # Conflict detected
            conflicts = True
            break
        active_blocks.append((block_id, departure))
    if conflicts:
        stops_with_conflicts.append(stop_id)
        print(f"Conflict detected at stop {stop_id}.")
    else:
        print(f"No conflict at stop {stop_id}.")

# Proceed to detailed processing for stops with conflicts
print("\nProcessing stops with conflicts...")

# Create DataFrame for each minute of the day
minutes_in_day = pd.DataFrame({
    'time_minute': range(0, 1440)
})
minutes_in_day['time_str'] = minutes_in_day['time_minute'].apply(lambda x: f"{x // 60:02d}:{x % 60:02d}")

# Process each stop with conflicts
for stop_id in stops_with_conflicts:
    stop_times_stop = stop_times[stop_times['stop_id'] == stop_id]
    blocks_at_stop = stop_times_stop['block_id'].unique()

    # Create a list to hold DataFrames for each block
    block_dfs = []

    for block_id in blocks_at_stop:
        block_stop_times = stop_times_stop[stop_times_stop['block_id'] == block_id]

        # Initialize DataFrame for the block
        block_df = pd.DataFrame({
            'time_minute': range(0, 1440),
            'time_str': [f"{x // 60:02d}:{x % 60:02d}" for x in range(0, 1440)],
            block_id: 0,
            'route_ids': [[] for _ in range(1440)]  # Initialize list of route_ids
        })

        # For each trip (including layovers)
        for _, row in block_stop_times.iterrows():
            start_minute = int(row['arrival_seconds'] // 60)
            end_minute = int(row['departure_seconds'] // 60)
            # Correct for times over midnight
            if end_minute < start_minute:
                end_minute += 1440
            # Mark occupancy and append route_ids
            for minute in range(start_minute, end_minute + 1):
                idx = minute % 1440  # Wrap around midnight
                block_df.at[idx, block_id] = 1  # Bus is present during this period
                block_df.at[idx, 'route_ids'].append(row['route_id'])

        # Remove duplicate route_ids in each list
        block_df['route_ids'] = block_df['route_ids'].apply(lambda x: sorted(set(x)))

        # Convert route_ids lists to comma-separated strings
        block_df['route_ids'] = block_df['route_ids'].apply(lambda x: ', '.join(x))

        # Rename route_ids column to include block_id
        block_df.rename(columns={'route_ids': f'route_ids_{block_id}'}, inplace=True)

        # Keep only relevant columns for this block
        block_df = block_df[['time_minute', 'time_str', block_id, f'route_ids_{block_id}']]

        block_dfs.append(block_df)

    # Merge all block DataFrames on time_minute and time_str
    if block_dfs:
        blocks_occupancy_df = block_dfs[0]
        for df in block_dfs[1:]:
            blocks_occupancy_df = blocks_occupancy_df.merge(df, on=['time_minute', 'time_str'], how='left')
    else:
        blocks_occupancy_df = minutes_in_day.copy()

    # Fill NaN with default values
    blocks_occupancy_df.fillna({'time_minute': 0, 'time_str': '00:00'}, inplace=True)
    occupancy_columns = [col for col in blocks_occupancy_df.columns if col not in ['time_minute', 'time_str']]
    block_cols = [col for col in occupancy_columns if not col.startswith('route_ids_')]

    # Calculate total buses present at each minute
    blocks_occupancy_df['Total_Buses'] = blocks_occupancy_df[block_cols].sum(axis=1)

    # Check for conflicts (Total_Buses > 1)
    has_conflicts = blocks_occupancy_df['Total_Buses'].gt(1).any()
    if has_conflicts:
        num_conflict_minutes = blocks_occupancy_df['Total_Buses'].gt(1).sum()
        print(f"Stop {stop_id} has {num_conflict_minutes} minute(s) with conflicts (Total_Buses > 1).")
    else:
        print(f"Stop {stop_id} has no conflicts (after detailed processing).")

    # Rearrange columns
    cols = ['time_minute', 'time_str', 'Total_Buses'] + occupancy_columns
    blocks_occupancy_df = blocks_occupancy_df[cols]

    # Export to Excel with each block as a separate sheet
    output_file = os.path.join(base_output_path, f"stop_{stop_id}_occupancy.xlsx")
    with pd.ExcelWriter(output_file) as writer:
        # Write total buses sheet
        blocks_occupancy_df[['time_minute', 'time_str', 'Total_Buses']].to_excel(writer, sheet_name='Total_Buses', index=False)
        # Write each block's occupancy and route_ids
        for block_id in blocks_at_stop:
            occupancy_col = block_id
            route_ids_col = f'route_ids_{block_id}'
            df = blocks_occupancy_df[['time_minute', 'time_str', occupancy_col, route_ids_col]]
            df.rename(columns={occupancy_col: 'Occupancy', route_ids_col: 'Route_IDs'}, inplace=True)
            df.to_excel(writer, sheet_name=f"Block_{block_id}", index=False)

    print(f"Occupancy data for stop {stop_id} has been written to {output_file}.")

print("\nProcessing completed.")

