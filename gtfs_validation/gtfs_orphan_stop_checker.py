#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import os

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# Input directory containing GTFS files
gtfs_input_path = r"C:\Path\To\Your\System\GTFS_Data" # Replace with your file path

# Output file for orphaned stops
output_file = r'\\your_file_path\\here\\orphaned_stops.csv'

# GTFS files to load
gtfs_files = {
    'stops': 'stops.txt',
    'stop_times': 'stop_times.txt',
    'trips': 'trips.txt'
}

# ==============================
# END OF CONFIGURATION SECTION
# ==============================

def check_input_files(base_path, files_dict):
    """
    Verify that all required GTFS files exist in the specified directory.
    """
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"The input directory {base_path} does not exist.")
    for key, file_name in files_dict.items():
        file_path = os.path.join(base_path, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The required GTFS file {file_name} does not exist in {base_path}.")

def load_gtfs_data(base_path, files_dict):
    """
    Load GTFS data files into Pandas DataFrames.
    """
    data = {}
    for key, file_name in files_dict.items():
        file_path = os.path.join(base_path, file_name)
        try:
            data[key] = pd.read_csv(file_path, low_memory=False)
            print(f"Loaded {file_name} with {len(data[key])} records.")
        except Exception as e:
            raise Exception(f"Error loading {file_name}: {e}")
    return data

def find_orphaned_stops(stops_df, stop_times_df):
    """
    Find stops that are not referenced in the stop_times file.
    """
    # Ensure required columns exist
    required_columns = ['stop_id']
    for df, name in [(stops_df, 'stops'), (stop_times_df, 'stop_times')]:
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing required columns in {name}.")
    
    # Get all stop_ids from stop_times
    used_stop_ids = stop_times_df['stop_id'].unique()
    
    # Find stops that are not in used_stop_ids
    orphaned_stops = stops_df[~stops_df['stop_id'].isin(used_stop_ids)]
    
    return orphaned_stops

def save_orphaned_stops(orphaned_stops, output_file):
    """
    Save the orphaned stops to a CSV file.
    """
    if orphaned_stops.empty:
        print("No orphaned stops found. No file created.")
    else:
        if os.path.exists(output_file):
            confirm = input(f"{output_file} exists. Overwrite? (y/n): ")
            if confirm.lower() != 'y':
                print("Process aborted.")
                return
        orphaned_stops.to_csv(output_file, index=False)
        print(f"Orphaned stops saved to {output_file}")

def main():
    try:
        # Check if all input files exist
        print("Checking input files...")
        check_input_files(gtfs_input_path, gtfs_files)
        print("All input files are present.")
        
        # Load GTFS data
        print("Loading GTFS data...")
        data = load_gtfs_data(gtfs_input_path, gtfs_files)
        print("GTFS data loaded successfully.")
        
        # Find orphaned stops
        print("Finding orphaned stops...")
        orphaned_stops = find_orphaned_stops(data['stops'], data['stop_times'])
        print(f"Found {len(orphaned_stops)} orphaned stops.")
        
        # Save orphaned stops to CSV
        print(f"Saving orphaned stops to {output_file}...")
        save_orphaned_stops(orphaned_stops, output_file)
        print("Process completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()


# In[ ]:




