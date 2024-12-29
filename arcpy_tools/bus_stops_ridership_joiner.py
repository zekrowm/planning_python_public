"""
--------------------------------------------------------------------------
    This script processes bus stop data by performing a spatial join with census blocks,
    merging with ridership data from an Excel file, and filtering out bus stops that do
    not have corresponding ridership data. The final outputs include updated shapefiles
    with ridership information and aggregated data by census block.
--------------------------------------------------------------------------
"""

import arcpy
import os
import csv
import pandas as pd

# --------------------------------------------------------------------------
# User-defined variables
# --------------------------------------------------------------------------
census_blocks = r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\data\tl_2024_51_tabblock20\tl_2024_51_tabblock20.shp"
# This can be either a .shp or a .txt (GTFS stops.txt)
bus_stops_input = r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\data\stopsByLine_asofSept2024\stopsByLine_asofSept2024.shp"
excel_file = r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\data\ridership_by_stop_2024_12_23\STOP_USAGE_(BY_STOP_ID)_2024_12_23.xlsx"

output_folder = r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\output"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Intermediate and final outputs
# If using GTFS, we will create a feature class from the stops.txt.
# Otherwise, if using a shapefile, we use it directly.
gtfs_stops_fc = os.path.join(output_folder, "bus_stops_generated.shp")
joined_fc = os.path.join(output_folder, "BusStops_JoinedBlocks.shp")
matched_joined_fc = os.path.join(output_folder, "BusStops_Matched_JoinedBlocks.shp")
output_csv = os.path.join(output_folder, "bus_stops_with_census_blocks.csv")
blocks_with_ridership_shp = os.path.join(output_folder, "census_blocks_with_ridership.shp")

# Field configuration:
# For GTFS input: fields are assumed to be "stop_code", "stop_id", "stop_name", "stop_lat", "stop_lon"
# For shapefile input: fields are assumed to "StopId", "StopNum", etc.
# Adjust as needed.

# For ridership data, Excel contains STOP_ID, STOP_NAME, XBOARDINGS, XALIGHTINGS.
# The final output expects a consistent set of fields. We'll standardize to "stop_code" for GTFS
# and "StopId" for shapefile. Ultimately, we need a common join key. 
# For this example, let's assume:
# - GTFS: We'll join on stop_code
# - Shapefile: We'll join on StopId
#
# We'll unify this by internally standardizing to "stop_code" for GTFS and "StopId" for shapefile.
# The Excel uses STOP_ID, so we'll map accordingly.

# Decide which approach to take based on file type
is_gtfs_input = bus_stops_input.lower().endswith(".txt")

# Overwrite outputs
arcpy.env.overwriteOutput = True

# TODO: Make unique Census feature ID into a constant - GEOID, GEOIDFQ, GEOID20, GEOIDFQ20 are common

# --------------------------------------------------------------------------
# Step 1: Create or identify the bus stops feature class
# --------------------------------------------------------------------------
if is_gtfs_input:
    # We have a GTFS stops.txt file. Convert it to a point feature class.
    arcpy.management.XYTableToPoint(
        in_table=bus_stops_input,
        out_feature_class=gtfs_stops_fc,
        x_field="stop_lon",
        y_field="stop_lat",
        coordinate_system=arcpy.SpatialReference(4326)  # WGS84
    )
    print("GTFS stops feature class created at:\n{}".format(gtfs_stops_fc))
    bus_stops_fc = gtfs_stops_fc

    # We'll export fields from this FC and also rename fields for consistency.
    # Fields to export to CSV after join:
    # We know GTFS stops have: stop_code, stop_id, stop_name, and after spatial join: GEOID20, GEOIDFQ20
    fields_to_export = ["stop_code", "stop_id", "stop_name", "GEOID20", "GEOIDFQ20"]

else:
    # We have a shapefile of bus stops directly
    bus_stops_fc = bus_stops_input
    print("Using existing bus stops shapefile:\n{}".format(bus_stops_fc))

    # Fields to export to CSV after join for shapefile scenario:
    # Assuming fields: StopId, StopNum, and after join: GEOID20, GEOIDFQ20
    fields_to_export = ["StopId", "StopNum", "GEOID20", "GEOIDFQ20"]

# --------------------------------------------------------------------------
# Step 2: Spatial Join - Join bus stops to census blocks
# --------------------------------------------------------------------------
arcpy.SpatialJoin_analysis(
    target_features=bus_stops_fc,
    join_features=census_blocks,
    out_feature_class=joined_fc,
    join_operation="JOIN_ONE_TO_ONE",
    join_type="KEEP_ALL",
    match_option="INTERSECT"
)
print("Spatial join completed. Joined feature class created at:\n{}".format(joined_fc))

# --------------------------------------------------------------------------
# Step 3: Export joined data to CSV
# --------------------------------------------------------------------------
with arcpy.da.SearchCursor(joined_fc, fields_to_export) as cursor, open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields_to_export)
    for row in cursor:
        writer.writerow(row)

print("CSV export completed. CSV file created at:\n{}".format(output_csv))

# --------------------------------------------------------------------------
# Step 4: Read ridership data from Excel and merge
# --------------------------------------------------------------------------
df_excel = pd.read_excel(excel_file)

# Recalculate TOTAL
df_excel['TOTAL'] = df_excel['XBOARDINGS'] + df_excel['XALIGHTINGS']

# Read the joined CSV
df_csv = pd.read_csv(output_csv)

# We need to merge these dataframes. The Excel uses STOP_ID as the key.
# For GTFS scenario: we have stop_code and stop_id in the CSV.
# For shapefile scenario: we have StopId in the CSV.
#
# We'll handle each scenario separately:

if is_gtfs_input:
    # GTFS scenario: We'll join on stop_code <-> STOP_ID (since Excel STOP_ID matches GTFS stop_code typically)
    df_excel['STOP_ID'] = df_excel['STOP_ID'].astype(str)
    df_csv['stop_code'] = df_csv['stop_code'].astype(str)
    df_joined = pd.merge(df_excel, df_csv, left_on='STOP_ID', right_on='stop_code', how='inner')
else:
    # Shapefile scenario: We'll join on StopId <-> STOP_ID
    df_excel['STOP_ID'] = df_excel['STOP_ID'].astype(str)
    df_csv['StopId'] = df_csv['StopId'].astype(str)
    df_joined = pd.merge(df_excel, df_csv, left_on='STOP_ID', right_on='StopId', how='inner')

print("Data merged successfully. Number of matched bus stops: {}".format(len(df_joined)))

# --------------------------------------------------------------------------
# Step 4a: Filter joined_fc to include only matched bus stops
# --------------------------------------------------------------------------
# Define the key field based on input type
key_field = 'stop_code' if is_gtfs_input else 'StopId'

# Extract unique keys from the joined dataframe
matched_keys = df_joined[key_field].dropna().unique().tolist()

if matched_keys:
    # Determine the field type
    fields = arcpy.ListFields(joined_fc, key_field)
    if not fields:
        print(f"Error: Field '{key_field}' not found in '{joined_fc}'. Exiting script.")
        exit()
    field_type = fields[0].type  # e.g., 'String', 'Integer', etc.

    # Prepare the SQL where clause based on field type
    field_delimited = arcpy.AddFieldDelimiters(joined_fc, key_field)

    if field_type in ['String', 'Guid', 'Date']:
        # String-based field types require values to be quoted
        formatted_keys = ["'{}'".format(k.replace("'", "''")) for k in matched_keys]
    elif field_type in ['Integer', 'SmallInteger', 'Double', 'Single', 'OID']:
        # Numeric field types do not require quotes
        formatted_keys = [str(k) for k in matched_keys]
    else:
        print(f"Unsupported field type '{field_type}' for field '{key_field}'. Exiting script.")
        exit()

    # Due to potential large number of keys, split into manageable chunks
    chunk_size = 999  # Adjust based on database limitations
    where_clauses = []
    for i in range(0, len(formatted_keys), chunk_size):
        chunk = formatted_keys[i:i + chunk_size]
        clause = "{} IN ({})".format(field_delimited, ", ".join(chunk))
        where_clauses.append(clause)
    # Combine clauses with OR
    full_where_clause = " OR ".join(where_clauses)

    print(f"Constructed WHERE clause for filtering: {full_where_clause[:200]}...")  # Print a snippet for verification

    # Create a feature layer
    arcpy.MakeFeatureLayer_management(joined_fc, "joined_lyr")

    # Select features that match the where clause
    try:
        arcpy.SelectLayerByAttribute_management("joined_lyr", "NEW_SELECTION", full_where_clause)
    except arcpy.ExecuteError:
        print("Failed to execute SelectLayerByAttribute. Please check the WHERE clause syntax.")
        print(f"WHERE clause attempted: {full_where_clause}")
        raise

    # Check if any features were selected
    selected_count = int(arcpy.GetCount_management("joined_lyr").getOutput(0))
    if selected_count == 0:
        print("No features matched the WHERE clause. Exiting script.")
        exit()
    else:
        print(f"Number of features selected: {selected_count}")

    # Export the selected features to a new shapefile
    arcpy.CopyFeatures_management("joined_lyr", matched_joined_fc)
    print("Filtered joined feature class with matched bus stops created at:\n{}".format(matched_joined_fc))

    # Update joined_fc to point to the filtered feature class
    joined_fc = matched_joined_fc
else:
    print("No matched bus stops found in Excel data. Exiting script.")
    exit()

# --------------------------------------------------------------------------
# Step 5: Update the Bus Stops Shapefile with Ridership Data
# --------------------------------------------------------------------------
# Add fields for ridership: XBOARD, XALIGHT, XTOTAL
ridership_fields = [
    ("XBOARD", "DOUBLE"),
    ("XALIGHT", "DOUBLE"),
    ("XTOTAL", "DOUBLE")
]

existing_fields = [f.name for f in arcpy.ListFields(joined_fc)]
for f_name, f_type in ridership_fields:
    if f_name not in existing_fields:
        arcpy.management.AddField(joined_fc, f_name, f_type)

print("Ridership fields added (if not existing).")

# Create a dictionary of keys to ridership
stop_ridership_dict = {}
for idx, row in df_joined.iterrows():
    code = row[key_field] if not pd.isna(row[key_field]) else None
    if code is not None:
        stop_ridership_dict[str(code)] = {
            'XBOARD': row['XBOARDINGS'],
            'XALIGHT': row['XALIGHTINGS'],
            'XTOTAL': row['TOTAL']
        }

with arcpy.da.UpdateCursor(joined_fc, [key_field, "XBOARD", "XALIGHT", "XTOTAL"]) as cursor:
    for r in cursor:
        code_val = str(r[0])
        if code_val in stop_ridership_dict:
            r[1] = stop_ridership_dict[code_val]['XBOARD']
            r[2] = stop_ridership_dict[code_val]['XALIGHT']
            r[3] = stop_ridership_dict[code_val]['XTOTAL']
            cursor.updateRow(r)
        else:
            # This should not occur as we've filtered matched features
            r[1] = 0
            r[2] = 0
            r[3] = 0
            cursor.updateRow(r)

print("Bus stops shapefile updated with ridership data at:\n{}".format(joined_fc))

# --------------------------------------------------------------------------
# Step 6: Aggregate ridership by GEOID20
# --------------------------------------------------------------------------
df_agg = df_joined.groupby('GEOID20', as_index=False).agg({
    'XBOARDINGS': 'sum',
    'XALIGHTINGS': 'sum',
    'TOTAL': 'sum'
})

print("Ridership data aggregated by GEOID20.")

# --------------------------------------------------------------------------
# Step 7: Create a new Census Blocks Shapefile with aggregated ridership
# --------------------------------------------------------------------------
arcpy.management.CopyFeatures(census_blocks, blocks_with_ridership_shp)

agg_fields = [
    ("XBOARD_SUM", "DOUBLE"),
    ("XALITE_SUM", "DOUBLE"),
    ("TOTAL_SUM", "DOUBLE")
]

existing_fields_blocks = [f.name for f in arcpy.ListFields(blocks_with_ridership_shp)]
for f_name, f_type in agg_fields:
    if f_name not in existing_fields_blocks:
        arcpy.management.AddField(blocks_with_ridership_shp, f_name, f_type)

print("Aggregation fields added to census blocks shapefile (if not existing).")

agg_dict = {}
for idx, row in df_agg.iterrows():
    geoid = row['GEOID20']
    agg_dict[geoid] = {
        'XBOARD_SUM': row['XBOARDINGS'],
        'XALITE_SUM': row['XALIGHTINGS'],
        'TOTAL_SUM': row['TOTAL']
    }

with arcpy.da.UpdateCursor(blocks_with_ridership_shp, ["GEOID20", "XBOARD_SUM", "XALITE_SUM", "TOTAL_SUM"]) as cursor:
    for r in cursor:
        geoid = r[0]
        if geoid in agg_dict:
            r[1] = agg_dict[geoid]['XBOARD_SUM']
            r[2] = agg_dict[geoid]['XALITE_SUM']
            r[3] = agg_dict[geoid]['TOTAL_SUM']
            cursor.updateRow(r)
        else:
            r[1] = 0
            r[2] = 0
            r[3] = 0
            cursor.updateRow(r)

print("Census blocks shapefile updated with aggregated ridership data at:\n{}".format(blocks_with_ridership_shp))
print("Process complete.")
