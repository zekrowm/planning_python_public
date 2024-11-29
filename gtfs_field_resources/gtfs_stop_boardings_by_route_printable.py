

import pandas as pd
import os
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font

# ==============================
# CONFIGURATION SECTION
# ==============================

# Input file paths for GTFS files
base_input_path = r'path_to_your_input_folder'
trips_file = os.path.join(base_input_path, "trips.txt")
stop_times_file = os.path.join(base_input_path, "stop_times.txt")
routes_file = os.path.join(base_input_path, "routes.txt")
stops_file = os.path.join(base_input_path, "stops.txt")
calendar_file = os.path.join(base_input_path, "calendar.txt")

# Output directory
base_output_path = r'path_to_your_output_folder'
if not os.path.exists(base_output_path):
    os.makedirs(base_output_path)

# Define schedule types and corresponding days in the calendar
# Format: {'Schedule Type': ['day1', 'day2', ...]}
schedule_types = {
    'Weekday': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'],
    'Saturday': ['saturday'],
    'Sunday': ['sunday'],
    # 'Friday': ['friday'],  # Uncomment if you have a unique Friday schedule
}

# Time windows for filtering trips
# Format: {'Schedule Type': {'Time Window Name': ('Start Time', 'End Time')}}
# Times should be in 'HH:MM' 24-hour format
time_windows = {
    'Weekday': {
        'full_day': ('00:00', '23:59'),
        'morning': ('06:00', '09:59'),
        'afternoon': ('14:00', '17:59'),
        # 'evening': ('18:00', '21:59'),  # Add as needed
    },
    'Saturday': {
        'midday': ('10:00', '13:59'),
        # Add more time windows for Saturday if needed
    },
    # 'Sunday': {  # Uncomment and customize for Sunday if needed
    #     'morning': ('08:00', '11:59'),
    #     'afternoon': ('12:00', '15:59'),
    # },
}

# Placeholder values
missing_time = "________"
comments_placeholder = "__________________"
max_column_width = 50

# ==============================
# END OF CONFIGURATION
# ==============================

# Load GTFS data
def load_data():
    try:
        trips = pd.read_csv(trips_file)
        stop_times = pd.read_csv(stop_times_file)
        routes = pd.read_csv(routes_file)
        stops = pd.read_csv(stops_file)
        calendar = pd.read_csv(calendar_file)

        # If 'timepoint' column does not exist, assume all stops are timepoints
        if 'timepoint' not in stop_times.columns:
            stop_times['timepoint'] = 1
    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(1)
    return trips, stop_times, routes, stops, calendar

trips, stop_times, routes, stops, calendar = load_data()

# Helper functions
def time_to_minutes(time_str):
    """
    Converts a time string in 'HH:MM:SS' or 'HH:MM' format to total minutes since midnight.
    """
    parts = time_str.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = map(int, parts)
    elif len(parts) == 2:
        hours, minutes = map(int, parts)
        seconds = 0
    else:
        return None
    return hours * 60 + minutes + seconds / 60

def hhmm_to_minutes(time_str):
    """
    Converts a time string in 'HH:MM' format to total minutes since midnight.
    """
    parts = time_str.split(":")
    if len(parts) == 2:
        hours, minutes = map(int, parts)
        return hours * 60 + minutes
    else:
        return None

def format_time(time_str):
    """
    Formats a time string to 'HH:MM' format, handling times over 24 hours.
    """
    total_minutes = time_to_minutes(time_str)
    if total_minutes is not None:
        hours = int(total_minutes // 60)
        minutes = int(total_minutes % 60)
        return f"{hours:02}:{minutes:02}"
    else:
        return time_str

# Export DataFrame to Excel with formatting
def export_to_excel(df, output_file, bold_rows=None):
    if df.empty:
        print(f"No data to export to {output_file}.")
        return
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Schedule')
        workbook = writer.book
        sheet = writer.sheets['Schedule']

        # Format headers and columns
        for col_num, col_name in enumerate(df.columns, start=1):
            col_letter = get_column_letter(col_num)
            try:
                max_length = max((len(str(value)) for value in df[col_name]), default=0)
                max_length = max(max_length, len(col_name))
                sheet.column_dimensions[col_letter].width = min(max_length + 2, max_column_width)
            except Exception as e:
                print(f"Error setting column width for {col_name}: {e}")

        # Bold rows where timepoint == 1 (Actual Time == missing_time)
        if bold_rows is not None:
            bold_font = Font(bold=True)
            for row_idx in bold_rows:
                for col_num in range(1, len(df.columns) + 1):
                    cell = sheet.cell(row=row_idx + 2, column=col_num)  # +2 accounts for header row and 1-indexing
                    cell.font = bold_font

# Generate schedule for each configuration
for schedule_type, days in schedule_types.items():
    relevant_services = calendar[calendar[days].all(axis=1)]['service_id']
    for route_short_name in route_short_names:
        route_ids = routes[routes['route_short_name'] == route_short_name]['route_id']
        relevant_trips = trips[
            (trips['route_id'].isin(route_ids)) & (trips['service_id'].isin(relevant_services))
        ]
        for direction_id in relevant_trips['direction_id'].unique():
            trips_in_direction = relevant_trips[relevant_trips['direction_id'] == direction_id]
            timepoints = stop_times[stop_times['trip_id'].isin(trips_in_direction['trip_id'])]
            timepoints['departure_minutes'] = timepoints['departure_time'].apply(time_to_minutes)
            timepoints['formatted_departure_time'] = timepoints['departure_time'].apply(format_time)

            if schedule_type in time_windows:
                for time_window_name, (start_time, end_time) in time_windows[schedule_type].items():
                    start_minutes = hhmm_to_minutes(start_time)
                    end_minutes = hhmm_to_minutes(end_time)

                    timepoints_filtered = timepoints[
                        (timepoints['departure_minutes'] >= start_minutes) &
                        (timepoints['departure_minutes'] <= end_minutes)
                    ]

                    if not timepoints_filtered.empty:
                        # Merge with trips to get route_id, service_id, direction_id
                        timepoints_filtered = timepoints_filtered.merge(
                            trips[['trip_id', 'route_id', 'service_id', 'trip_headsign', 'direction_id']],
                            on='trip_id', how='left')

                        # Merge with stops to get stop_name
                        timepoints_filtered = timepoints_filtered.merge(
                            stops[['stop_id', 'stop_name']], on='stop_id', how='left')

                        # Merge with routes to get route_short_name
                        timepoints_filtered = timepoints_filtered.merge(
                            routes[['route_id', 'route_short_name']], on='route_id', how='left')

                        # Create 'Trip ID + Start Time'
                        # For each trip_id, get the first 'departure_time'
                        first_departure_times = timepoints_filtered.groupby('trip_id')['departure_minutes'].min().reset_index()
                        first_departure_times.rename(columns={'departure_minutes': 'trip_start_minutes'}, inplace=True)
                        timepoints_filtered = timepoints_filtered.merge(first_departure_times, on='trip_id', how='left')

                        timepoints_filtered['trip_start_time'] = timepoints_filtered['trip_start_minutes'].apply(
                            lambda x: f"{int(x // 60):02}:{int(x % 60):02}")

                        timepoints_filtered['Trip ID + Start Time'] = timepoints_filtered['trip_id'].astype(str) + ' ' + timepoints_filtered['trip_start_time']

                        # Map 'direction_id' to 'Direction' (e.g., 0 to 'Outbound', 1 to 'Inbound')
                        direction_mapping = {0: 'Outbound', 1: 'Inbound'}
                        timepoints_filtered['Direction'] = timepoints_filtered['direction_id'].map(direction_mapping)

                        # Add 'Service Period' as 'schedule_type'
                        timepoints_filtered['Service Period'] = schedule_type

                        # Add 'Route' from 'route_short_name'
                        timepoints_filtered['Route'] = timepoints_filtered['route_short_name']

                        # Create 'Scheduled Time' from 'formatted_departure_time'
                        timepoints_filtered['Scheduled Time'] = timepoints_filtered['formatted_departure_time']

                        # Create 'Actual Time' column
                        timepoints_filtered['Actual Time'] = timepoints_filtered['timepoint'].apply(
                            lambda x: missing_time if x == 1 else 'X')

                        # 'Stop Sequence' from 'stop_sequence'
                        timepoints_filtered['Stop Sequence'] = timepoints_filtered['stop_sequence']

                        # 'Stop ID' from 'stop_id'
                        timepoints_filtered['Stop ID'] = timepoints_filtered['stop_id']

                        # 'Stop Name' from 'stop_name'
                        timepoints_filtered['Stop Name'] = timepoints_filtered['stop_name']

                        # 'Date' as placeholder
                        timepoints_filtered['Date'] = missing_time

                        # Determine first and last stops for 'On' and 'Off' columns
                        first_stops = timepoints_filtered.groupby('trip_id')['stop_sequence'].min().reset_index()
                        first_stops.rename(columns={'stop_sequence': 'first_stop_sequence'}, inplace=True)
                        last_stops = timepoints_filtered.groupby('trip_id')['stop_sequence'].max().reset_index()
                        last_stops.rename(columns={'stop_sequence': 'last_stop_sequence'}, inplace=True)
                        timepoints_filtered = timepoints_filtered.merge(first_stops, on='trip_id', how='left')
                        timepoints_filtered = timepoints_filtered.merge(last_stops, on='trip_id', how='left')

                        # 'On' column
                        def on_value(row):
                            if row['stop_sequence'] == row['last_stop_sequence']:
                                return 'X'
                            else:
                                return missing_time

                        timepoints_filtered['On'] = timepoints_filtered.apply(on_value, axis=1)

                        # 'Off' column
                        def off_value(row):
                            if row['stop_sequence'] == row['first_stop_sequence']:
                                return 'X'
                            else:
                                return missing_time

                        timepoints_filtered['Off'] = timepoints_filtered.apply(off_value, axis=1)

                        # Now, select the required columns in the required order
                        columns_order = ['Date', 'Service Period', 'Route', 'Direction', 'Trip ID + Start Time',
                                         'Scheduled Time', 'Actual Time', 'Stop Sequence', 'Stop ID', 'Stop Name', 'On', 'Off']

                        # Prepare the output DataFrame
                        output_df = timepoints_filtered[columns_order]

                        # Sort the DataFrame
                        output_df['trip_start_minutes'] = timepoints_filtered['trip_start_minutes']
                        output_df = output_df.sort_values(by=['trip_start_minutes', 'Trip ID + Start Time', 'Stop Sequence'])
                        output_df = output_df.reset_index(drop=True)

                        # Remove the temporary 'trip_start_minutes' column
                        output_df = output_df.drop(columns=['trip_start_minutes'])

                        # Get indices of rows where 'Actual Time' == missing_time for bold formatting
                        bold_rows = output_df[output_df['Actual Time'] == missing_time].index.tolist()

                        # Export to Excel
                        output_file = os.path.join(
                            base_output_path,
                            f"{route_short_name}_{schedule_type}_{time_window_name}_dir_{direction_id}.xlsx"
                        )
                        export_to_excel(output_df, output_file, bold_rows=bold_rows)
            else:
                print(f"No time windows specified for schedule type '{schedule_type}'. Skipping.")


