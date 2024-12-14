#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
import os
import re
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

# ===========================================
# START USER CONFIGURATION
# ===========================================
file_config = {
    r"C:\Path\To\Your\Input_File_1.xlsx": { # Replace with real file path
        "checker_name": "Tom Jones",        # Replace with real name
        "date": "2024-12-03"                # Replace with real date of data collection
    },
    r"C:\Path\To\Your\Input_File_2.xlsx": { # Replace with real file path
        "checker_name": "Jane Doe",         # Replace with real name
        "date": "2024-12-04"                # Replace with real date of data collection
    },
    r"C:\Path\To\Your\Input_File_3.xlsx": { # Replace with real file path
        "checker_name": "John Smith",       # Replace with real name
        "date": "2024-12-05"                # Replace with real date of data collection
    }
}

output_dir = r"C:\Path\To\Your\Output_Folder" # Replace with real folder path
os.makedirs(output_dir, exist_ok=True)

concatenated_output_file = os.path.join(output_dir, "concatenated_data.xlsx") # Output file name for valid time data
invalid_times_output_file = os.path.join(output_dir, "invalid_times.xlsx")  # Output file name for invalid time data
summary_output_file = os.path.join(output_dir, "summary.xlsx")  # Output file name for data summary

# Users can modify this list to generate summary sheets based on different variable column names
# Defaults are ['route_short_name', 'date', 'checker_name']
summary_variables = [
    'route_short_name',
    'date',
    'checker_name'
    ]

# ===========================================
# END USER CONFIGURATION
# ===========================================

# HELPER FUNCTIONS

def parse_time_str(t):
    if pd.isna(t):
        return t
    t_str = str(t)
    
    # Remove spaces, colons, and non-numeric characters
    t_clean = re.sub(r'[^0-9]', '', t_str)
    
    # Backfill zeros to ensure 4 digits
    if len(t_clean) == 3:
        t_clean = '0' + t_clean
    elif len(t_clean) < 4:
        t_clean = t_clean.zfill(4)
    elif len(t_clean) > 4:
        # If more than 4 digits, assume it's malformed and return original
        return t_str
    
    # Ensure it has exactly 4 digits now
    if len(t_clean) != 4:
        return t_str
    
    hour_part = t_clean[:2]
    minute_part = t_clean[2:]
    
    try:
        hour = int(hour_part)
        minute = int(minute_part)
        if 0 <= hour < 24 and 0 <= minute < 60:
            return f"{hour:02d}:{minute:02d}"
        else:
            return t_str  # Return original if invalid time
    except ValueError:
        return t_str  # Return original if conversion fails

def clean_route_short_name(route):
    if pd.isna(route):
        return None
    return str(route).strip().replace(" ", "")

def adjust_time_if_needed(scheduled_time_str, actual_time_str):
    """
    Adjusts the actual_time by adding 12 hours if:
    - The difference between actual_time and scheduled_time is > 11 hours
    - Adding 12 hours to actual_time brings it within 20 minutes of scheduled_time
    """
    try:
        scheduled_time = datetime.strptime(scheduled_time_str, "%H:%M")
        actual_time = datetime.strptime(actual_time_str, "%H:%M")
    except (ValueError, TypeError):
        # If parsing fails, return the original actual_time_str
        return actual_time_str

    # Calculate the time difference in minutes
    diff = (actual_time - scheduled_time).total_seconds() / 60
    diff_abs = abs(diff)

    if diff_abs > 11 * 60:
        # Add 12 hours to actual_time
        adjusted_time = actual_time + timedelta(hours=12)
        # Recalculate the difference
        new_diff = (adjusted_time - scheduled_time).total_seconds() / 60
        new_diff_abs = abs(new_diff)
        if new_diff_abs <= 20:
            # Return the adjusted time in "HH:MM" format
            return adjusted_time.strftime("%H:%M")
    return actual_time_str

def adjust_times_based_on_scheduled(df, time_pairs, time_diff_threshold=11*60, adjustment_threshold=20):
    """
    Adjust actual times based on scheduled times.
    
    Parameters:
    - df: DataFrame containing the data
    - time_pairs: List of tuples containing (scheduled_time_col, actual_time_col)
    - time_diff_threshold: Threshold in minutes to consider a time as potentially off by 12 hours
    - adjustment_threshold: Threshold in minutes to confirm the adjustment
    """
    for scheduled_col, actual_col in time_pairs:
        if scheduled_col in df.columns and actual_col in df.columns:
            # Apply the adjustment row-wise
            df[actual_col] = df.apply(
                lambda row: adjust_time_if_needed(row[scheduled_col], row[actual_col]) 
                if pd.notna(row[scheduled_col]) and pd.notna(row[actual_col]) else row[actual_col],
                axis=1
            )
    return df

def is_valid_time(time_str):
    """
    Checks if a string is a valid HH:MM time format.
    
    Returns True if valid, False otherwise.
    """
    if pd.isna(time_str):
        return False
    if not isinstance(time_str, str):
        return False
    match = re.match(r'^(\d{2}):(\d{2})$', time_str)
    if not match:
        return False
    hour, minute = int(match.group(1)), int(match.group(2))
    return 0 <= hour < 24 and 0 <= minute < 60

# MAIN PROCESSING
dataframes = {}
invalid_times_rows = []  # List to collect invalid rows
original_column_order = None  # To store the column order from the first file

# Explicitly list all time-like columns to ensure they are processed
time_like_cols = [
    "act_arrival",
    "act_departure",
    "departure_time",
    "arrival_time"
    # Add any additional time columns here
]

# Define your time pairs here: (scheduled_time_col, actual_time_col)
# Update these pairs based on your actual column names
# For this example, I'll assume:
# - 'arrival_time' corresponds to 'act_arrival'
# - 'departure_time' corresponds to 'act_departure'
# Adjust as necessary
time_pairs = [
    ("arrival_time", "act_arrival"),
    ("departure_time", "act_departure"),
    # Add more pairs as needed
]

# Read files and parse time-like columns
for idx, (fpath, meta) in enumerate(file_config.items()):
    if not os.path.exists(fpath):
        print(f"Warning: File not found {fpath}. Skipping.")
        continue

    try:
        df = pd.read_excel(fpath)
    except Exception as e:
        print(f"Error reading '{fpath}': {e}")
        continue

    # Capture the column order from the first processed file
    if original_column_order is None:
        original_column_order = list(df.columns)

    df['checker_name'] = meta['checker_name']
    df['date'] = meta['date']

    # Ensure 'checker_name' and 'date' are at the end
    # Remove them from their current position if they exist
    if 'checker_name' in df.columns:
        cols = [col for col in df.columns if col != 'checker_name'] + ['checker_name']
        df = df[cols]
    if 'date' in df.columns:
        cols = [col for col in df.columns if col != 'date'] + ['date']
        df = df[cols]

    # Process only the explicitly listed time-like columns
    for col in time_like_cols:
        if col in df.columns:
            original_values = df[col].copy()
            df[col] = df[col].apply(parse_time_str)
            # Optional: Print changes for debugging
            changed = df[col] != original_values
            if changed.any():
                print(f"Processed column '{col}' in file '{fpath}'.")

    # Clean route_short_name if exists
    if 'route_short_name' in df.columns:
        df['route_short_name'] = df['route_short_name'].apply(clean_route_short_name)

    # Adjust actual times based on scheduled times
    df = adjust_times_based_on_scheduled(df, time_pairs, time_diff_threshold=11*60, adjustment_threshold=20)

    # Identify invalid rows where both act_arrival and act_departure are invalid or blank
    # Define the columns to check
    arrival_col = 'act_arrival'
    departure_col = 'act_departure'

    # Apply the is_valid_time function to both columns
    valid_arrival = df[arrival_col].apply(is_valid_time)
    valid_departure = df[departure_col].apply(is_valid_time)

    # Rows where both are invalid
    invalid_rows_mask = ~valid_arrival & ~valid_departure

    # Extract invalid rows
    invalid_rows = df[invalid_rows_mask]
    if not invalid_rows.empty:
        print(f"Found {len(invalid_rows)} invalid rows in file '{fpath}'.")
        invalid_times_rows.append(invalid_rows)

    # Store the processed DataFrame
    dataframes[fpath] = df

if not dataframes:
    print("No files processed.")
    exit(1)

# Combine all invalid rows into a single DataFrame
if invalid_times_rows:
    invalid_times_df = pd.concat(invalid_times_rows, ignore_index=True)
    # Optionally, you can add a source file column to trace back
    invalid_times_df['source_file'] = invalid_times_df.apply(
        lambda row: next((k for k, v in file_config.items() if v['checker_name'] == row['checker_name'] and v['date'] == row['date']), None),
        axis=1
    )
else:
    invalid_times_df = pd.DataFrame()  # Empty DataFrame if no invalid rows found

# Determine common columns based on the first file's column order
common_columns = original_column_order.copy()

# Append any new columns that were added ('checker_name' and 'date') at the end
additional_columns = [col for col in dataframes[list(dataframes.keys())[0]].columns if col not in common_columns]
common_columns += additional_columns

# Concatenate on common columns only
concatenated = pd.concat([
    df.reindex(columns=common_columns + [col for col in df.columns if col not in common_columns])
    for df in dataframes.values()
], ignore_index=True)

# Function to reorder columns, keeping original order and appending new columns at the end
def reorder_columns(df, original_order, new_columns=[]):
    ordered_cols = [col for col in original_order if col in df.columns]
    ordered_cols += [col for col in df.columns if col not in ordered_cols and col not in new_columns]
    ordered_cols += new_columns
    return df[ordered_cols]

# Reorder concatenated DataFrame columns
concatenated = reorder_columns(concatenated, original_column_order, new_columns=['checker_name', 'date'])

# Reorder invalid_times_df columns
if not invalid_times_df.empty:
    # Exclude 'source_file' from the original order
    invalid_times_df = reorder_columns(invalid_times_df, original_column_order, new_columns=['checker_name', 'date', 'source_file'])

# Function to adjust column widths in Excel
def adjust_excel_column_widths(writer, df, sheet_name):
    worksheet = writer.sheets[sheet_name]
    for i, col in enumerate(df.columns, start=1):
        # Calculate the maximum length of the column's data
        max_len = df[col].astype(str).map(len).max()
        # Compare with the header length
        max_len = max(max_len, len(col))
        # Set the column width with some padding
        worksheet.column_dimensions[get_column_letter(i)].width = max_len + 2

# Export concatenated data to Excel
with pd.ExcelWriter(concatenated_output_file, engine="openpyxl") as writer:
    concatenated.to_excel(writer, index=False, sheet_name="Sheet1")
    adjust_excel_column_widths(writer, concatenated, "Sheet1")

print(f"Concatenated data saved to: {concatenated_output_file}")

# Export invalid times to a separate Excel file if any invalid rows exist
if not invalid_times_df.empty:
    with pd.ExcelWriter(invalid_times_output_file, engine="openpyxl") as writer:
        invalid_times_df.to_excel(writer, index=False, sheet_name="Invalid_Times")
        adjust_excel_column_widths(writer, invalid_times_df, "Invalid_Times")
    print(f"Invalid times data saved to: {invalid_times_output_file}")
else:
    print("No invalid time entries found.")

# SUMMARY STATISTICS EXPORT

# Only proceed if there are valid arrival times to summarize
# Valid arrival rows have both scheduled and actual arrival times in 'HH:MM' format
valid_summary_mask = concatenated['arrival_time'].apply(is_valid_time) & concatenated['act_arrival'].apply(is_valid_time)
valid_summary_df = concatenated[valid_summary_mask].copy()

if not valid_summary_df.empty:
    # Convert 'arrival_time' and 'act_arrival' to datetime objects for calculation
    valid_summary_df['scheduled_arrival_dt'] = pd.to_datetime(valid_summary_df['arrival_time'], format='%H:%M', errors='coerce')
    valid_summary_df['actual_arrival_dt'] = pd.to_datetime(valid_summary_df['act_arrival'], format='%H:%M', errors='coerce')
    
    # Calculate the difference in minutes: actual - scheduled
    valid_summary_df['arrival_diff_minutes'] = (valid_summary_df['actual_arrival_dt'] - valid_summary_df['scheduled_arrival_dt']).dt.total_seconds() / 60
    
    # Categorize arrivals
    def categorize_arrival(diff):
        if diff < -1:
            return 'Early'
        elif -1 <= diff <= 5:
            return 'On-Time'
        elif diff > 5:
            return 'Late'
        else:
            return 'Uncategorized'  # Fallback for any unexpected cases

    valid_summary_df['arrival_category'] = valid_summary_df['arrival_diff_minutes'].apply(categorize_arrival)
    
    # Aggregate overall counts
    category_counts = valid_summary_df['arrival_category'].value_counts().to_dict()
    
    # Ensure all categories are present
    categories = ['Early', 'On-Time', 'Late']
    summary_data = {category: category_counts.get(category, 0) for category in categories}
    
    # Calculate total valid arrivals
    total_valid = sum(summary_data.values())
    
    # Calculate percentages
    summary_percentages = {category: (count / total_valid) * 100 if total_valid > 0 else 0 for category, count in summary_data.items()}
    
    # Create overall summary DataFrame
    summary_df = pd.DataFrame({
        'Category': categories,
        'Count': [summary_data[cat] for cat in categories],
        'Percentage': [f"{summary_percentages[cat]:.2f}%" for cat in categories]
    })
    
    # SUMMARY PER VARIABLE EXPORT

    # Initialize a list to collect per-variable summaries
    per_variable_summaries = []

    for var in summary_variables:
        if var not in valid_summary_df.columns:
            print(f"Warning: Variable '{var}' not found in the data. Skipping summary for this variable.")
            continue

        # Group by the variable and arrival_category to get counts
        per_var_counts = valid_summary_df.groupby([var, 'arrival_category']).size().unstack(fill_value=0)
        
        # Calculate percentages per group
        per_var_percent = per_var_counts.div(per_var_counts.sum(axis=1), axis=0) * 100
        
        # Ensure all categories are present in the columns
        for category in categories:
            if category not in per_var_counts.columns:
                per_var_counts[category] = 0
                per_var_percent[category] = 0.0
        
        # Reorder columns to match categories
        per_var_counts = per_var_counts[categories]
        per_var_percent = per_var_percent[categories]
        
        # Create a summary per variable DataFrame with counts and percentages
        summary_per_var_df = per_var_counts.copy()
        for category in categories:
            summary_per_var_df[f"{category}_Percentage"] = per_var_percent[category].round(2).astype(str) + '%'
        
        # Reset index to have the variable as a column
        summary_per_var_df = summary_per_var_df.reset_index()
        
        # Rename count columns for clarity
        summary_per_var_df.rename(columns={
            var: var.replace('_', ' ').title(),
            'Early': 'Early_Count',
            'On-Time': 'On-Time_Count',
            'Late': 'Late_Count'
        }, inplace=True)
        
        # Rename percentage columns for clarity
        summary_per_var_df.rename(columns={
            'Early_Percentage': 'Early_Percentage',
            'On-Time_Percentage': 'On-Time_Percentage',
            'Late_Percentage': 'Late_Percentage'
        }, inplace=True)
        
        # Arrange columns in a logical order
        ordered_columns = [
            var.replace('_', ' ').title(),
            'Early_Count',
            'Early_Percentage',
            'On-Time_Count',
            'On-Time_Percentage',
            'Late_Count',
            'Late_Percentage'
        ]
        summary_per_var_df = summary_per_var_df[ordered_columns]
        
        # Store the summary DataFrame with its corresponding variable
        per_variable_summaries.append((var, summary_per_var_df))
    
    # ===========================================
    # EXPORT SUMMARY TO EXCEL
    # ===========================================
    with pd.ExcelWriter(summary_output_file, engine="openpyxl") as writer:
        # Write overall summary to 'Summary' sheet
        summary_df.to_excel(writer, index=False, sheet_name="Summary")
        adjust_excel_column_widths(writer, summary_df, "Summary")
        
        # Write per-variable summaries to separate sheets
        for var, summary_per_var_df in per_variable_summaries:
            sheet_name = f"Summary_by_{var}"
            # Ensure sheet name length does not exceed Excel's limit (31 characters)
            if len(sheet_name) > 31:
                sheet_name = f"Summary_by_{var[:28]}"
            summary_per_var_df.to_excel(writer, index=False, sheet_name=sheet_name)
            adjust_excel_column_widths(writer, summary_per_var_df, sheet_name)
    
    print(f"Summary statistics saved to: {summary_output_file}")
else:
    print("No valid arrival time entries found for summary.")


# In[ ]:




