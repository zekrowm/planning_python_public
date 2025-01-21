#!/usr/bin/env python
# coding: utf-8

"""
Census Data Processing Script

This script processes census data by performing the following operations:
- Loads and merges shapefiles from specified input directories.
- Filters geographic data based on provided FIPS codes.
- Processes various demographic datasets including population, households, jobs, income, ethnicity, language proficiency, vehicle ownership, and age.
- Merges tract-level and block-level data to calculate estimates.
- Exports the processed data to CSV and shapefile formats for further analysis.

Configuration:
- Customize the full file paths in the configuration section below.
- JT00, P1, H9, and .shp files are mandatory.
- Other tables are optional depending on the detailed information you are interested in.
- Output configuration is consolidated at the end of the config section.
"""

import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# ---------------
# 1) MANDATORY INPUTS
# ---------------

# A. Shapefiles (block-level)
BLOCK_SHP_FILES = [
    r"C:\full\path\to\tl_2023_11_tabblock20\tl_2023_11_tabblock20.shp",
    r"C:\full\path\to\tl_2023_24_tabblock20\tl_2023_24_tabblock20.shp",
    r"C:\full\path\to\tl_2023_51_tabblock20\tl_2023_51_tabblock20.shp"
    # Add or remove as needed
]

# B. FIPS codes to filter
FIPS_TO_FILTER = [
    '51059', '51013', '51510', '51600', '51610', '11001',
    '24031', '24033', '51107', '51153', '51683', '51685'
    # Add or remove as needed
]

# C. Population data by block (P1)
P1_FILES = [
    r"C:\full\path\to\DECENNIALPL2020.P1_2024-04-18T092750\DECENNIALPL2020.P1-Data.csv",
    r"C:\full\path\to\DECENNIALPL2020.P1_2024-04-18T092825\DECENNIALPL2020.P1-Data.csv",
    r"C:\full\path\to\DECENNIALPL2020.P1_2024-04-18T092847\DECENNIALPL2020.P1-Data.csv"
]

# D. Households data by block (H9)
H9_FILES = [
    r"C:\full\path\to\DECENNIALDHC2020.H9_2024-04-18T134122\DECENNIALDHC2020.H9-Data.csv",
    r"C:\full\path\to\DECENNIALDHC2020.H9_2024-04-18T130317\DECENNIALDHC2020.H9-Data.csv",
    r"C:\full\path\to\DECENNIALDHC2020.H9_2024-04-18T130535\DECENNIALDHC2020.H9-Data.csv"
]
DTYPES_H9 = {'GEO_ID': str, 'H9_001N': 'Int64'}

# E. Jobs data by block (JT00)
JT00_FILES = [
    r"C:\full\path\to\va_wac_S000_JT00_2021.csv.gz",
    r"C:\full\path\to\md_wac_S000_JT00_2021.csv.gz",
    r"C:\full\path\to\dc_wac_S000_JT00_2021.csv.gz"
]

# ---------------
# 2) OPTIONAL INPUTS
# ---------------

# Income data by tract (B19001)
INCOME_B19001_FILES = [
    r"C:\full\path\to\ACSDT5Y2022.B19001_2024-09-05T191138\ACSDT5Y2022.B19001-Data.csv"
    # Add more paths or remove if not used
]

# Ethnicity data by tract (P9)
ETHNICITY_P9_FILES = [
    r"C:\full\path\to\DECENNIALDHC2020.P9_2024-09-05T191411\DECENNIALDHC2020.P9-Data.csv"
    # Add more paths or remove if not used
]

# Language data by tract (C16001)
LANGUAGE_C16001_FILES = [
    r"C:\full\path\to\ACSDT5Y2022.C16001_2024-09-05T191721\ACSDT5Y2022.C16001-Data.csv"
    # Add more paths or remove if not used
]

# Vehicle ownership data by tract (B08201)
VEHICLE_B08201_FILES = [
    r"C:\full\path\to\ACSDT5Y2022.B08201_2024-09-05T192107\ACSDT5Y2022.B08201-Data.csv"
    # Add more paths or remove if not used
]

# Age data by tract (B01001)
AGE_B01001_FILES = [
    r"C:\full\path\to\ACSDT5Y2022.B01001_2024-09-05T192719\ACSDT5Y2022.B01001-Data.csv"
    # Add more paths or remove if not used
]

# ---------------
# 3) OUTPUT CONFIGURATION
# ---------------

CSV_OUTPUT_PATH = r"C:\full\path\to\output\df_joined_blocks.csv"

# Combined SHP output path (folder + filename)
SHP_OUTPUT_PATH = r"C:\Users\zach\Desktop\Zach\python_stuff\projects\census_data_processing_for_transit_2025_01_21\output\va_md_dc_census_blocks_folder\va_census_blocks.shp"

# ===================================
# END OF CONFIGURATION SECTION
# ===================================

# ----------------------- SHP DATA BY BLOCK ------------------------------
# Load and merge all shapefiles into a single GeoDataFrame
gdf_list = [
    gpd.read_file(shp_file) 
    for shp_file in BLOCK_SHP_FILES
]
merged_gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs=gdf_list[0].crs)

# Create a new 'FIPS' column and filter based on FIPS codes
merged_gdf['FIPS'] = merged_gdf['STATEFP20'].astype(str) + merged_gdf['COUNTYFP20'].astype(str)
filtered_gdf = merged_gdf[merged_gdf['FIPS'].isin(FIPS_TO_FILTER)]

# Plot the filtered shapefile
filtered_gdf.plot()
plt.title("Shapefile Plot - Filtered by FIPS")
plt.show()

# ----------------------- POPULATION DATA BY BLOCK (P1) ------------------------------
column_mapping_p1 = {'GEO_ID': 'GEO_ID', 'NAME': 'NAME', 'P1_001N': 'total_pop'}

df_population = pd.concat([
    pd.read_csv(csv_file, skiprows=[1])
      .rename(columns=column_mapping_p1)[list(column_mapping_p1.values())]
    for csv_file in P1_FILES
], ignore_index=True)

# --------------------- HOUSEHOLDS DATA BY BLOCK (H9) ------------------------------
df_household = pd.concat([
    pd.read_csv(file, dtype=DTYPES_H9, skiprows=[1])
      .rename(columns={'H9_001N': 'total_hh'})[['GEO_ID', 'total_hh']]
    for file in H9_FILES
], ignore_index=True)

# --------------------- JOBS DATA BY BLOCK (JT00) ------------------------------
df_jobs = pd.concat([
    pd.read_csv(file, compression='gzip').rename(columns={
        "w_geocode": "w_geocode",
        "C000": "tot_empl",
        "CE01": "low_wage",
        "CE02": "mid_wage",
        "CE03": "high_wage"
    }).assign(GEO_ID=lambda df: '1000000US' + df['w_geocode'].astype(str))[
        ['GEO_ID', 'tot_empl', 'low_wage', 'mid_wage', 'high_wage']
    ]
    for file in JT00_FILES
], ignore_index=True)

# ----------------------- INCOME DATA BY TRACT (B19001) [OPTIONAL] ------------------------------
column_mapping_income = {
    'GEO_ID': 'GEO_ID',
    'NAME': 'NAME',
    'B19001_001E': 'total_hh',
    'B19001_002E': 'sub_10k',
    'B19001_003E': '10k_15k',
    'B19001_004E': '15k_20k',
    'B19001_005E': '20k_25k',
    'B19001_006E': '25k_30k',
    'B19001_007E': '30k_35k',
    'B19001_008E': '35k_40k',
    'B19001_009E': '40k_45k',
    'B19001_010E': '45k_50k',
    'B19001_011E': '50k_60k'
}

df_income = pd.DataFrame()
if INCOME_B19001_FILES:
    df_income = pd.concat([
        pd.read_csv(f, skiprows=[1])
          .rename(columns=column_mapping_income)[list(column_mapping_income.values())]
        for f in INCOME_B19001_FILES
    ], ignore_index=True)
    df_income['low_income'] = df_income[[
        'sub_10k', '10k_15k', '15k_20k', '20k_25k', '25k_30k',
        '30k_35k', '35k_40k', '40k_45k', '45k_50k', '50k_60k'
    ]].sum(axis=1)
    df_income['perc_low_income'] = df_income['low_income'] / df_income['total_hh']
    df_income['FIPS_code'] = df_income['GEO_ID'].str[9:14]
    df_income = df_income.drop(['total_hh'], axis=1)

# ----------------------- ETHNICITY DATA BY TRACT (P9) [OPTIONAL] ------------------------------
column_mapping_ethnicity = {
    'GEO_ID': 'GEO_ID',
    'NAME': 'NAME',
    'P9_001N': 'total_pop',
    'P9_002N': 'all_hisp',
    'P9_005N': 'white',
    'P9_006N': 'black',
    'P9_007N': 'native',
    'P9_008N': 'asian',
    'P9_009N': 'pac_isl',
    'P9_010N': 'other',
    'P9_011N': 'multi'
}

df_ethnicity = pd.DataFrame()
if ETHNICITY_P9_FILES:
    df_ethnicity = pd.concat([
        pd.read_csv(f, skiprows=[1])
          .rename(columns=column_mapping_ethnicity)[list(column_mapping_ethnicity.values())]
        for f in ETHNICITY_P9_FILES
    ], ignore_index=True)
    df_ethnicity['minority'] = df_ethnicity[['black', 'native', 'asian', 'pac_isl', 'other', 'multi']].sum(axis=1)
    df_ethnicity['perc_minority'] = df_ethnicity['minority'] / df_ethnicity['total_pop']
    df_ethnicity['FIPS_code'] = df_ethnicity['GEO_ID'].str[9:14]
    df_ethnicity = df_ethnicity.drop(['total_pop'], axis=1)

# ----------------------- LANGUAGE DATA BY TRACT (C16001) [OPTIONAL] ------------------------------
df_language = pd.DataFrame()
if LANGUAGE_C16001_FILES:
    # Concatenate all language files if multiple are provided
    df_language = pd.concat([
        pd.read_csv(lep_file, dtype={'GEO_ID': str}, skiprows=[1]).rename(columns={
            'C16001_001E': 'total_lang_pop',
            'C16001_005E': 'spanish_engnwell',
            'C16001_008E': 'frenchetc_engnwell',
            'C16001_011E': 'germanetc_engnwell',
            'C16001_014E': 'slavicetc_engnwell',
            'C16001_017E': 'indoeuroetc_engnwell',
            'C16001_020E': 'korean_engnwell',
            'C16001_023E': 'chineseetc_engnwell',
            'C16001_026E': 'vietnamese_engnwell',
            'C16001_032E': 'asiapacetc_engnwell',
            'C16001_035E': 'arabic_engnwell',
            'C16001_037E': 'otheretc_engnwell'
        })[
            ['GEO_ID', 'total_lang_pop', 'spanish_engnwell', 'frenchetc_engnwell',
             'germanetc_engnwell', 'slavicetc_engnwell', 'indoeuroetc_engnwell',
             'korean_engnwell', 'chineseetc_engnwell', 'vietnamese_engnwell',
             'asiapacetc_engnwell', 'arabic_engnwell', 'otheretc_engnwell']
        ]
        for lep_file in LANGUAGE_C16001_FILES
    ], ignore_index=True)
    
    # Convert columns to numeric, sum LEP values, and calculate percentages
    lep_columns = [
        'spanish_engnwell', 'frenchetc_engnwell', 'germanetc_engnwell', 'slavicetc_engnwell',
        'indoeuroetc_engnwell', 'korean_engnwell', 'chineseetc_engnwell', 'vietnamese_engnwell',
        'asiapacetc_engnwell', 'arabic_engnwell', 'otheretc_engnwell'
    ]
    
    df_language[lep_columns] = df_language[lep_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
    df_language['all_nwell'] = df_language[lep_columns].sum(axis=1)
    df_language['perc_lep'] = df_language['all_nwell'] / df_language['total_lang_pop']
    df_language['perc_lep'] = df_language['perc_lep'].replace([float('inf'), -float('inf')], 0).fillna(0).round(3)

# ----------------------- VEHICLE OWNERSHIP DATA BY TRACT (B08201) [OPTIONAL] ------------------------------
column_mapping_vehicle = {
    'GEO_ID': 'GEO_ID',
    'B08201_001E': 'all_hhs',
    'B08201_002E': 'veh_0_all_hh',
    'B08201_003E': 'veh_1_all_hh',
    'B08201_008E': 'veh_0_hh_1',
    'B08201_009E': 'veh_1_hh_1',
    'B08201_014E': 'veh_0_hh_2',
    'B08201_015E': 'veh_1_hh_2',
    'B08201_020E': 'veh_0_hh_3',
    'B08201_021E': 'veh_1_hh_3',
    'B08201_022E': 'veh_2_hh_3',
    'B08201_026E': 'veh_0_hh_4p',
    'B08201_027E': 'veh_1_hh_4p',
    'B08201_028E': 'veh_2_hh_4p'
}

df_vehicle = pd.DataFrame()
if VEHICLE_B08201_FILES:
    df_vehicle = pd.concat([
        pd.read_csv(f, skiprows=[1])
          .rename(columns=column_mapping_vehicle)[list(column_mapping_vehicle.values())]
        for f in VEHICLE_B08201_FILES
    ], ignore_index=True)
    df_vehicle['all_lo_veh_hh'] = df_vehicle[['veh_0_all_hh', 'veh_1_all_hh']].sum(axis=1)
    df_vehicle['perc_lo_veh'] = df_vehicle['all_lo_veh_hh'] / df_vehicle['all_hhs']
    df_vehicle['perc_0_veh'] = df_vehicle['veh_0_all_hh'] / df_vehicle['all_hhs']
    df_vehicle['perc_1_veh'] = df_vehicle['veh_1_all_hh'] / df_vehicle['all_hhs']
    df_vehicle['perc_veh_1_hh_1'] = df_vehicle['veh_1_hh_1'] / df_vehicle['all_hhs']
    df_vehicle['perc_lo_veh_mod'] = df_vehicle['perc_lo_veh'] - df_vehicle['perc_veh_1_hh_1']
    df_vehicle['perc_lo_veh_mod'] = df_vehicle['perc_lo_veh_mod'].round(3)

# ----------------------- AGE DATA BY TRACT (B01001) [OPTIONAL] ------------------------------
column_mapping_age = {
    'GEO_ID': 'GEO_ID',
    'B01001_001E': 'total_pop',
    'B01001_006E': 'm_15_17',
    'B01001_007E': 'm_18_19',
    'B01001_008E': 'm_20',
    'B01001_009E': 'm_21',
    'B01001_020E': 'm_65_66',
    'B01001_021E': 'm_67_69',
    'B01001_022E': 'm_70_74',
    'B01001_023E': 'm_75_79',
    'B01001_024E': 'm_80_84',
    'B01001_025E': 'm_a_85',
    'B01001_030E': 'f_15_17',
    'B01001_031E': 'f_18_19',
    'B01001_032E': 'f_20',
    'B01001_033E': 'f_21',
    'B01001_044E': 'f_65_66',
    'B01001_045E': 'f_67_69',
    'B01001_046E': 'f_70_74',
    'B01001_047E': 'f_75_79',
    'B01001_048E': 'f_80_84',
    'B01001_049E': 'f_a_85'
}

df_age = pd.DataFrame()
if AGE_B01001_FILES:
    df_age = pd.concat([
        pd.read_csv(f, skiprows=[1])
          .rename(columns=column_mapping_age)[list(column_mapping_age.values())]
        for f in AGE_B01001_FILES
    ], ignore_index=True)
    df_age['all_youth'] = df_age[['m_15_17', 'f_15_17', 'm_18_19', 'f_18_19', 'm_20', 'f_20', 'm_21', 'f_21']].sum(axis=1)
    df_age['all_elderly'] = df_age[[
        'm_65_66', 'f_65_66', 'm_67_69', 'f_67_69', 'm_70_74', 'f_70_74',
        'm_75_79', 'f_75_79', 'm_80_84', 'f_80_84', 'm_a_85', 'f_a_85'
    ]].sum(axis=1)
    df_age['perc_youth'] = (df_age['all_youth'] / df_age['total_pop']).round(3)
    df_age['perc_elderly'] = (df_age['all_elderly'] / df_age['total_pop']).round(3)
    df_age = df_age.drop(['total_pop'], axis=1)

# ----------------------- JOIN TRACT-LEVEL DATA ---------------------------
# Merge all tract-level dataframes (some may be empty)
df_tracts = pd.DataFrame()
# Start with income if not empty
if not df_income.empty:
    df_tracts = df_income
if not df_ethnicity.empty:
    # If df_tracts is empty, just assign it; otherwise merge
    df_tracts = df_ethnicity if df_tracts.empty else pd.merge(df_tracts, df_ethnicity, on='GEO_ID', how='outer')
if not df_vehicle.empty:
    df_tracts = df_vehicle if df_tracts.empty else pd.merge(df_tracts, df_vehicle, on='GEO_ID', how='outer')
if not df_age.empty:
    df_tracts = df_age if df_tracts.empty else pd.merge(df_tracts, df_age, on='GEO_ID', how='outer')
if not df_language.empty:
    df_tracts = df_language if df_tracts.empty else pd.merge(df_tracts, df_language, on='GEO_ID', how='outer')

df_tracts.fillna(0, inplace=True)

# Example columns to drop if you want to trim detail. Adjust as needed if you have optional data missing.
columns_to_drop = [
    'sub_10k', '10k_15k', '15k_20k', '20k_25k', '25k_30k', '30k_35k',
    '35k_40k', '40k_45k', '45k_50k', '50k_60k',
    'veh_0_hh_1', 'veh_1_hh_1', 'veh_0_hh_2', 'veh_1_hh_2', 'veh_0_hh_3',
    'veh_1_hh_3', 'veh_2_hh_3', 'veh_0_hh_4p', 'veh_1_hh_4p', 'veh_2_hh_4p',
    'm_15_17', 'm_18_19', 'm_20', 'm_21', 'm_65_66', 'm_67_69', 'm_70_74',
    'm_75_79', 'm_80_84', 'm_a_85', 'f_15_17', 'f_18_19', 'f_20', 'f_21',
    'f_65_66', 'f_67_69', 'f_70_74', 'f_75_79', 'f_80_84', 'f_a_85',
    'all_youth', 'all_elderly', 'all_hisp', 'white', 'black', 'native',
    'asian', 'pac_isl', 'other', 'multi', 'minority', 'total_lang_pop',
    'spanish_engnwell', 'frenchetc_engnwell', 'germanetc_engnwell',
    'slavicetc_engnwell', 'indoeuroetc_engnwell', 'korean_engnwell',
    'chineseetc_engnwell', 'vietnamese_engnwell', 'asiapacetc_engnwell',
    'arabic_engnwell', 'otheretc_engnwell', 'all_nwell', 'low_income',
    'all_hhs', 'veh_0_all_hh', 'veh_1_all_hh', 'all_lo_veh_hh'
]
# Drop only if columns exist (handles optional sets not present)
df_tracts.drop(columns=[col for col in columns_to_drop if col in df_tracts], inplace=True, errors='ignore')

# ----------------------- JOIN BLOCK-LEVEL DATA ---------------------------
df_blocks = pd.merge(df_population, df_household, on='GEO_ID', how='outer')
df_blocks = pd.merge(df_blocks, df_jobs, on='GEO_ID', how='outer')

# Modify 'GEO_ID' and create synthetic tract/block IDs
df_blocks['tract_id_synth'] = df_blocks['GEO_ID'].str[9:20]
df_blocks['block_id_synth'] = df_blocks['GEO_ID'].str[9:24]
df_blocks.fillna(0, inplace=True)

# ----------------------- CREATE CLEAN TRACT ID IN TRACT-LEVEL DATA ---------------------------
if not df_tracts.empty:
    df_tracts['tract_id_clean'] = df_tracts['GEO_ID'].str[9:]

# ----------------------- JOIN BLOCK AND TRACT DATA USING TRACT ID ---------------------------
if not df_tracts.empty:
    df_combined = pd.merge(df_blocks, df_tracts, left_on='tract_id_synth', right_on='tract_id_clean', how='outer')
else:
    df_combined = df_blocks.copy()

df_combined.fillna(0, inplace=True)

# ----------------------- CALCULATE BLOCK-LEVEL ESTIMATES (IF OPTIONAL DATA EXISTS) ---------------------------
estimate_cols = [
    'est_low_income', 'est_lep', 'est_minority', 'est_lo_veh',
    'est_lo_veh_mod', 'est_youth', 'est_elderly'
]

if 'perc_low_income' in df_combined.columns and 'total_hh' in df_combined.columns:
    df_combined['est_low_income'] = df_combined['perc_low_income'] * df_combined['total_hh']
if 'perc_lep' in df_combined.columns and 'total_pop' in df_combined.columns:
    df_combined['est_lep'] = df_combined['perc_lep'] * df_combined['total_pop']
if 'perc_minority' in df_combined.columns and 'total_pop' in df_combined.columns:
    df_combined['est_minority'] = df_combined['perc_minority'] * df_combined['total_pop']
if 'perc_lo_veh' in df_combined.columns and 'total_hh' in df_combined.columns:
    df_combined['est_lo_veh'] = df_combined['perc_lo_veh'] * df_combined['total_hh']
if 'perc_lo_veh_mod' in df_combined.columns and 'total_hh' in df_combined.columns:
    df_combined['est_lo_veh_mod'] = df_combined['perc_lo_veh_mod'] * df_combined['total_hh']
if 'perc_youth' in df_combined.columns and 'total_pop' in df_combined.columns:
    df_combined['est_youth'] = df_combined['perc_youth'] * df_combined['total_pop']
if 'perc_elderly' in df_combined.columns and 'total_pop' in df_combined.columns:
    df_combined['est_elderly'] = df_combined['perc_elderly'] * df_combined['total_pop']

# Round the estimates to 3 decimal places
for col in estimate_cols:
    if col in df_combined.columns:
        df_combined[col] = df_combined[col].round(3)

# ----------------------- FILTER BLOCK DATA TO NEEDED JURISDICTIONS ---------------------------
# Adjust 'GEO_ID' format if necessary
if 'GEO_ID_x' in df_combined.columns:
    df_combined['GEO_ID'] = df_combined['GEO_ID_x'].str[:2] + '1' + df_combined['GEO_ID_x'].str[3:]
else:
    df_combined['GEO_ID'] = df_combined['GEO_ID']

df_combined['tract_id_synth'] = df_combined['GEO_ID'].str[9:20]
df_combined['block_id_synth'] = df_combined['GEO_ID'].str[9:24]
df_combined['FIPS_code'] = df_combined['GEO_ID'].str[9:14]

df_filtered_blocks = df_combined[df_combined['FIPS_code'].isin(FIPS_TO_FILTER)].copy()

# Recalculate final estimates for filtered blocks (optional, if needed):
for col in estimate_cols:
    if col in df_filtered_blocks.columns:
        df_filtered_blocks[col] = df_filtered_blocks[col].round(3)

# ------------------- CONVERT DATA TYPES IF NEEDED ---------------------------
for column in df_filtered_blocks.columns:
    if pd.api.types.is_extension_array_dtype(df_filtered_blocks[column]):
        df_filtered_blocks[column] = df_filtered_blocks[column].astype('float64')

# ------------------- EXPORT TO CSV AND SHAPEFILE ---------------------------
# Export CSV
os.makedirs(os.path.dirname(CSV_OUTPUT_PATH), exist_ok=True)
df_filtered_blocks.to_csv(CSV_OUTPUT_PATH, index=True)
print(f"CSV file saved to: {CSV_OUTPUT_PATH}")

# Export Shapefile
os.makedirs(os.path.dirname(SHP_OUTPUT_PATH), exist_ok=True)
shapefile_output_path = SHP_OUTPUT_PATH

# Join the block data to the filtered shapefile on 'GEO_ID'
# Ensure that 'GEOIDFQ20' exists in shapefile; adjust if necessary
if 'GEOIDFQ20' in filtered_gdf.columns and 'GEO_ID' in df_filtered_blocks.columns:
    result_gdf = filtered_gdf.merge(df_filtered_blocks, left_on='GEOIDFQ20', right_on='GEO_ID', how='left')
else:
    # If 'GEOIDFQ20' does not exist, adjust the key accordingly or skip merging
    print("Warning: 'GEOIDFQ20' or 'GEO_ID' not found for merging shapefile. Shapefile will be exported without joined data.")
    result_gdf = filtered_gdf.copy()

result_gdf.to_file(shapefile_output_path)
print(f"Shapefile saved to: {shapefile_output_path}")
