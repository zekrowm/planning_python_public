"""
--------------------------------------------------------------------------
    This script processes bus stop data by performing a spatial join
    with census blocks, merging with ridership data from an Excel file,
    and filtering out bus stops that do not have corresponding ridership
    data. The final outputs include updated shapefiles with ridership
    information and aggregated data by census block.
--------------------------------------------------------------------------
"""

import sys
import os
import csv

import arcpy
import pandas as pd

# --------------------------------------------------------------------------
# User-defined variables
# --------------------------------------------------------------------------
CENSUS_BLOCKS = (
    r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\data\tl_2024_51_tabblock20"
    r"\tl_2024_51_tabblock20.shp"
)
# This can be either a .shp or a .txt (GTFS stops.txt)
BUS_STOPS_INPUT = (
    r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\data"
    r"\stopsByLine_asofSept2024\stopsByLine_asofSept2024.shp"
)
EXCEL_FILE = (
    r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\data"
    r"\ridership_by_stop_2024_12_23\STOP_USAGE_(BY_STOP_ID)_2024_12_23.xlsx"
)

OUTPUT_FOLDER = r"G:\projects\dot\zkrohmal\ridership_by_stop_for_dwayne\output"
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Intermediate and final outputs
# If using GTFS, we will create a feature class from the stops.txt.
# Otherwise, if using a shapefile, we use it directly.
GTFS_STOPS_FC = os.path.join(OUTPUT_FOLDER, "bus_stops_generated.shp")
JOINED_FC = os.path.join(OUTPUT_FOLDER, "BusStops_JoinedBlocks.shp")
MATCHED_JOINED_FC = os.path.join(OUTPUT_FOLDER, "BusStops_Matched_JoinedBlocks.shp")
OUTPUT_CSV = os.path.join(OUTPUT_FOLDER, "bus_stops_with_census_blocks.csv")
BLOCKS_WITH_RIDERSHIP_SHP = os.path.join(OUTPUT_FOLDER, "census_blocks_with_ridership.shp")

# Field configuration:
# For GTFS input: fields are assumed to be "stop_code", "stop_id", "stop_name", "stop_lat", "stop_lon"
# For shapefile input: fields are assumed to "StopId", "StopNum", etc.
# Adjust as needed.

# For ridership data, Excel contains STOP_ID, STOP_NAME, XBOARDINGS, XALIGHTINGS.
# The final output expects a consistent set of fields. We'll standardize to "stop_code" for GTFS
# and "StopId" for shapefile. Ultimately, we need a common join key.

# Decide which approach to take based on file type
IS_GTFS_INPUT = BUS_STOPS_INPUT.lower().endswith(".txt")

# Overwrite outputs
arcpy.env.overwriteOutput = True

# FIXME: Make unique Census feature ID into a constant - GEOID, GEOIDFQ, GEOID20, GEOIDFQ20 are common

# --------------------------------------------------------------------------
# Step 1: Create or identify the bus stops feature class
# --------------------------------------------------------------------------
if IS_GTFS_INPUT:
    # We have a GTFS stops.txt file. Convert it to a point feature class.
    arcpy.management.XYTableToPoint(
        in_table=BUS_STOPS_INPUT,
        out_feature_class=GTFS_STOPS_FC,
        x_field="stop_lon",
        y_field="stop_lat",
        coordinate_system=arcpy.SpatialReference(4326)  # WGS84
    )
    print("GTFS stops feature class created at:\n{}".format(GTFS_STOPS_FC))
    bus_stops_fc = GTFS_STOPS_FC

    # We'll export fields from this FC and also rename fields for consistency.
    fields_to_export = ["stop_code", "stop_id", "stop_name", "GEOID20", "GEOIDFQ20"]
else:
    # We have a shapefile of bus stops directly
    bus_stops_fc = BUS_STOPS_INPUT
    print("Using existing bus stops shapefile:\n{}".format(bus_stops_fc))

    fields_to_export = ["StopId", "StopNum", "GEOID20", "GEOIDFQ20"]

# --------------------------------------------------------------------------
# Step 2: Spatial Join - Join bus stops to census blocks
# --------------------------------------------------------------------------
arcpy.SpatialJoin_analysis(
    target_features=bus_stops_fc,
    join_features=CENSUS_BLOCKS,
    out_feature_class=JOINED_FC,
    join_operation="JOIN_ONE_TO_ONE",
    join_type="KEEP_ALL",
    match_option="INTERSECT"
)
print("Spatial join completed. Joined feature class created at:\n{}".format(JOINED_FC))

# --------------------------------------------------------------------------
# Step 3: Export joined data to CSV
# --------------------------------------------------------------------------
with arcpy.da.SearchCursor(JOINED_FC, fields_to_export) as cursor, \
        open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields_to_export)
    for row in cursor:
        writer.writerow(row)

print("CSV export completed. CSV file created at:\n{}".format(OUTPUT_CSV))

# --------------------------------------------------------------------------
# Step 4: Read ridership data from Excel and merge
# --------------------------------------------------------------------------
df_excel = pd.read_excel(EXCEL_FILE)
df_excel['TOTAL'] = df_excel['XBOARDINGS'] + df_excel['XALIGHTINGS']

df_csv = pd.read_csv(OUTPUT_CSV)

if IS_GTFS_INPUT:
    df_excel['STOP_ID'] = df_excel['STOP_ID'].astype(str)
    df_csv['stop_code'] = df_csv['stop_code'].astype(str)
    df_joined = pd.merge(df_excel, df_csv, left_on='STOP_ID', right_on='stop_code',
                         how='inner')
else:
    df_excel['STOP_ID'] = df_excel['STOP_ID'].astype(str)
    df_csv['StopId'] = df_csv['StopId'].astype(str)
    df_joined = pd.merge(df_excel, df_csv, left_on='STOP_ID', right_on='StopId',
                         how='inner')

print("Data merged successfully. Number of matched bus stops: {}".format(len(df_joined)))

# --------------------------------------------------------------------------
# Step 4a: Filter JOINED_FC to include only matched bus stops
# --------------------------------------------------------------------------
key_field = 'stop_code' if IS_GTFS_INPUT else 'StopId'
matched_keys = df_joined[key_field].dropna().unique().tolist()

if matched_keys:
    fields = arcpy.ListFields(JOINED_FC, key_field)
    if not fields:
        print("Error: Field '{}' not found in '{}'. Exiting script.".format(key_field, JOINED_FC))
        sys.exit()

    field_type = fields[0].type  # e.g., 'String', 'Integer', etc.
    field_delimited = arcpy.AddFieldDelimiters(JOINED_FC, key_field)

    if field_type in ['String', 'Guid', 'Date']:
        formatted_keys = ["'{}'".format(k.replace("'", "''")) for k in matched_keys]
    elif field_type in ['Integer', 'SmallInteger', 'Double', 'Single', 'OID']:
        formatted_keys = [str(k) for k in matched_keys]
    else:
        print("Unsupported field type '{}' for field '{}'. Exiting script.".format(field_type, key_field))
        sys.exit()

    CHUNK_SIZE = 999
    where_clauses = []
    for i in range(0, len(formatted_keys), CHUNK_SIZE):
        chunk = formatted_keys[i:i + CHUNK_SIZE]
        clause = "{} IN ({})".format(field_delimited, ", ".join(chunk))
        where_clauses.append(clause)

    full_where_clause = " OR ".join(where_clauses)
    print("Constructed WHERE clause for filtering: {}...".format(full_where_clause[:200]))

    arcpy.MakeFeatureLayer_management(JOINED_FC, "joined_lyr")

    try:
        arcpy.SelectLayerByAttribute_management("joined_lyr", "NEW_SELECTION",
                                                full_where_clause)
    except arcpy.ExecuteError:
        print("Failed to execute SelectLayerByAttribute. Please check the WHERE clause syntax.")
        print("WHERE clause attempted: {}".format(full_where_clause))
        raise

    selected_count = int(arcpy.GetCount_management("joined_lyr").getOutput(0))
    if selected_count == 0:
        print("No features matched the WHERE clause. Exiting script.")
        sys.exit()
    else:
        print("Number of features selected: {}".format(selected_count))

    arcpy.CopyFeatures_management("joined_lyr", MATCHED_JOINED_FC)
    print("Filtered joined feature class with matched bus stops created at:\n{}".format(MATCHED_JOINED_FC))

    joined_fc = MATCHED_JOINED_FC
else:
    print("No matched bus stops found in Excel data. Exiting script.")
    sys.exit()

# --------------------------------------------------------------------------
# Step 5: Update the Bus Stops Shapefile with Ridership Data
# --------------------------------------------------------------------------
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
arcpy.management.CopyFeatures(CENSUS_BLOCKS, BLOCKS_WITH_RIDERSHIP_SHP)

agg_fields = [
    ("XBOARD_SUM", "DOUBLE"),
    ("XALITE_SUM", "DOUBLE"),
    ("TOTAL_SUM", "DOUBLE")
]

existing_fields_blocks = [f.name for f in arcpy.ListFields(BLOCKS_WITH_RIDERSHIP_SHP)]
for f_name, f_type in agg_fields:
    if f_name not in existing_fields_blocks:
        arcpy.management.AddField(BLOCKS_WITH_RIDERSHIP_SHP, f_name, f_type)

print("Aggregation fields added to census blocks shapefile (if not existing).")

agg_dict = {}
for idx, row in df_agg.iterrows():
    geoid = row['GEOID20']
    agg_dict[geoid] = {
        'XBOARD_SUM': row['XBOARDINGS'],
        'XALITE_SUM': row['XALIGHTINGS'],
        'TOTAL_SUM': row['TOTAL']
    }

with arcpy.da.UpdateCursor(
    BLOCKS_WITH_RIDERSHIP_SHP,
    ["GEOID20", "XBOARD_SUM", "XALITE_SUM", "TOTAL_SUM"]
) as cursor:
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

print(
    "Census blocks shapefile updated with aggregated ridership data at:\n"
    "{}".format(BLOCKS_WITH_RIDERSHIP_SHP)
)
print("Process complete.")
