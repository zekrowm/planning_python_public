#!/usr/bin/env python
# coding: utf-8

import os
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# ==============================
# CONFIGURATION SECTION - CUSTOMIZE HERE
# ==============================

# Base input and output folders
BASE_INPUT_FOLDER = r'\\your_file_path\here\\'
BASE_OUTPUT_FOLDER = r'\\your_file_path\here\\'

# SHP data by tract (relative to BASE_INPUT_FOLDER)
input_files = [
    "tl_2023_11_tabblock20/tl_2023_11_tabblock20.shp",
    "tl_2023_24_tabblock20/tl_2023_24_tabblock20.shp",
    "tl_2023_51_tabblock20/tl_2023_51_tabblock20.shp"
    # Add, subtract, replace with your desired state(s)
]

# Jurisdiction FIPS codes to filter
fips_to_filter = [
    '51059', '51013', '51510', '51600', '51610', '11001',
    '24031', '24033', '51107', '51153', '51683', '51685'
    # Add, subtract, replace with your desired jurisdiction(s)
]

# Population data by block (P1)
csv_files_p1 = [
    "DECENNIALPL2020.P1_2024-04-18T092750/DECENNIALPL2020.P1-Data.csv",
    "DECENNIALPL2020.P1_2024-04-18T092825/DECENNIALPL2020.P1-Data.csv",
    "DECENNIALPL2020.P1_2024-04-18T092847/DECENNIALPL2020.P1-Data.csv"
    # Replace with your folder and file name(s)
]

# Households data by block (H9)
file_paths_h9 = [
    "DECENNIALDHC2020.H9_2024-04-18T134122/DECENNIALDHC2020.H9-Data.csv",
    "DECENNIALDHC2020.H9_2024-04-18T130317/DECENNIALDHC2020.H9-Data.csv",
    "DECENNIALDHC2020.H9_2024-04-18T130535/DECENNIALDHC2020.H9-Data.csv"
    # Replace with your folder and file name(s)
]
dtypes_h9 = {'GEO_ID': str, 'H9_001N': 'Int64'}

# Jobs data by block (JT00)
base_path_jobs = BASE_INPUT_FOLDER  # Use the base input folder

# Income data by tract (B19001)
csv_files_income = [
    "ACSDT5Y2022.B19001_2024-09-05T191138/ACSDT5Y2022.B19001-Data.csv"
    # Replace with your folder and file name(s)
]

# Ethnicity data by tract (P9)
csv_files_ethnicity = [
    "DECENNIALDHC2020.P9_2024-09-05T191411/DECENNIALDHC2020.P9-Data.csv"
    # Replace with your folder and file name(s)
]

# Language data by tract (C16001)
lep_data_file = [
    "ACSDT5Y2022.C16001_2024-09-05T191721/ACSDT5Y2022.C16001-Data.csv"
    # Replace with your folder and file name(s)
]

# Vehicle ownership data by tract (B08201)
csv_files_vehicle = [
    "ACSDT5Y2022.B08201_2024-09-05T192107/ACSDT5Y2022.B08201-Data.csv"
    # Replace with your folder and file name(s)
]

# Age data by tract (B01001)
csv_files_age = [
    "ACSDT5Y2022.B01001_2024-09-05T192719/ACSDT5Y2022.B01001-Data.csv"
    # Replace with your folder and file name(s)
]

# Output paths (relative to BASE_OUTPUT_FOLDER)
CSV_OUTPUT_FILENAME = 'df_joined_blocks.csv' # Replace with your preferred file name
SHAPEFILE_FOLDER_NAME = "va_md_dc_census_blocks_folder" # Replace with your preferred folder name

# ===================================
# END OF CONFIGURATION SECTION
# ===================================

# ----------------------- SHP DATA BY TRACT ------------------------------
# Load and merge all shapefiles into a single GeoDataFrame
gdf_list = [gpd.read_file(os.path.join(BASE_INPUT_FOLDER, file)) for file in input_files]
merged_gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs=gdf_list[0].crs)

# Create a new 'FIPS' column and filter based on FIPS codes
merged_gdf['FIPS'] = merged_gdf['STATEFP20'].astype(str) + merged_gdf['COUNTYFP20'].astype(str)
filtered_gdf = merged_gdf[merged_gdf['FIPS'].isin(fips_to_filter)]

# Plot the filtered shapefile
filtered_gdf.plot()
plt.title("Shapefile Plot - Filtered by FIPS")
plt.show()

# ----------------------- POPULATION DATA BY BLOCK (P1) ------------------------------
column_mapping_p1 = {'GEO_ID': 'GEO_ID', 'NAME': 'NAME', 'P1_001N': 'total_pop'}

# Read and combine CSV files for population data
df_population = pd.concat([
    pd.read_csv(os.path.join(BASE_INPUT_FOLDER, f), skiprows=[1]).rename(columns=column_mapping_p1)[list(column_mapping_p1.values())]
    for f in csv_files_p1], ignore_index=True)

# --------------------- HOUSEHOLDS DATA BY BLOCK (H9) ------------------------------
# Read and process household data
df_household = pd.concat([
    pd.read_csv(os.path.join(BASE_INPUT_FOLDER, file), dtype=dtypes_h9, skiprows=[1]).rename(columns={'H9_001N': 'total_hh'})[['GEO_ID', 'total_hh']]
    for file in file_paths_h9], ignore_index=True)

# --------------------- JOBS DATA BY BLOCK (JT00) ------------------------------
# File paths for jobs data by state
df_jobs = pd.concat([
    pd.read_csv(os.path.join(base_path_jobs, f"{state}_wac_S000_JT00_2021.csv.gz"), compression='gzip').rename(columns={
        "w_geocode": "w_geocode",
        "C000": "tot_empl",
        "CE01": "low_wage",
        "CE02": "mid_wage",
        "CE03": "high_wage"
    }).assign(GEO_ID=lambda df: '1000000US' + df['w_geocode'].astype(str))[
        ['GEO_ID', 'tot_empl', 'low_wage', 'mid_wage', 'high_wage']
    ] for state in ['va', 'md', 'dc']
], ignore_index=True)

# ----------------------- INCOME DATA BY TRACT (B19001) ------------------------------
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

# Read and process income data
df_income = pd.concat([pd.read_csv(os.path.join(BASE_INPUT_FOLDER, f), skiprows=[1]).rename(columns=column_mapping_income)[list(column_mapping_income.values())]
                       for f in csv_files_income], ignore_index=True)

# Calculate total low-income households and percentage
df_income['low_income'] = df_income[['sub_10k', '10k_15k', '15k_20k', '20k_25k', '25k_30k', '30k_35k',
                                     '35k_40k', '40k_45k', '45k_50k', '50k_60k']].sum(axis=1)
df_income['perc_low_income'] = df_income['low_income'] / df_income['total_hh']
df_income['FIPS_code'] = df_income['GEO_ID'].str[9:14]
df_income = df_income.drop(['total_hh'], axis=1)

# ----------------------- ETHNICITY DATA BY TRACT (P9) ------------------------------
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

# Read and process ethnicity data
df_ethnicity = pd.concat([pd.read_csv(os.path.join(BASE_INPUT_FOLDER, f), skiprows=[1]).rename(columns=column_mapping_ethnicity)[list(column_mapping_ethnicity.values())]
                          for f in csv_files_ethnicity], ignore_index=True)

# Calculate minority population and percentage
df_ethnicity['minority'] = df_ethnicity[['black', 'native', 'asian', 'pac_isl', 'other', 'multi']].sum(axis=1)
df_ethnicity['perc_minority'] = df_ethnicity['minority'] / df_ethnicity['total_pop']
df_ethnicity['FIPS_code'] = df_ethnicity['GEO_ID'].str[9:14]
df_ethnicity = df_ethnicity.drop(['total_pop'], axis=1)

# ----------------------- LANGUAGE DATA BY TRACT (C16001) ------------------------------
# Read and process language proficiency data
df_language = pd.read_csv(os.path.join(BASE_INPUT_FOLDER, lep_data_file), dtype={'GEO_ID': str}, skiprows=[1])
df_language = df_language.rename(columns={
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
})

# Select only the renamed columns (drop all others)
columns_to_keep = ['GEO_ID', 'total_lang_pop'] + [
    'spanish_engnwell', 'frenchetc_engnwell', 'germanetc_engnwell', 'slavicetc_engnwell',
    'indoeuroetc_engnwell', 'korean_engnwell', 'chineseetc_engnwell', 'vietnamese_engnwell',
    'asiapacetc_engnwell', 'arabic_engnwell', 'otheretc_engnwell'
]
df_language = df_language[columns_to_keep]

# Convert columns to numeric, sum LEP values, and calculate percentages
lep_columns = ['spanish_engnwell', 'frenchetc_engnwell', 'germanetc_engnwell', 'slavicetc_engnwell',
               'indoeuroetc_engnwell', 'korean_engnwell', 'chineseetc_engnwell', 'vietnamese_engnwell',
               'asiapacetc_engnwell', 'arabic_engnwell', 'otheretc_engnwell']

df_language[lep_columns] = df_language[lep_columns].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
df_language['all_nwell'] = df_language[lep_columns].sum(axis=1)
df_language['perc_lep'] = df_language['all_nwell'] / df_language['total_lang_pop']
df_language['perc_lep'] = df_language['perc_lep'].replace([float('inf'), -float('inf')], 0).fillna(0)
df_language['perc_lep'] = df_language['perc_lep'].round(3)

# ----------------------- VEHICLE OWNERSHIP DATA BY TRACT (B08201) ------------------------------
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

# Read and process vehicle ownership data
df_vehicle = pd.concat([pd.read_csv(os.path.join(BASE_INPUT_FOLDER, f), skiprows=[1]).rename(columns=column_mapping_vehicle)[list(column_mapping_vehicle.values())]
                        for f in csv_files_vehicle], ignore_index=True)

# Calculate total low-vehicle households and percentages
df_vehicle['all_lo_veh_hh'] = df_vehicle[['veh_0_all_hh', 'veh_1_all_hh']].sum(axis=1)
df_vehicle['perc_lo_veh'] = df_vehicle['all_lo_veh_hh'] / df_vehicle['all_hhs']
df_vehicle['perc_0_veh'] = df_vehicle['veh_0_all_hh'] / df_vehicle['all_hhs']
df_vehicle['perc_1_veh'] = df_vehicle['veh_1_all_hh'] / df_vehicle['all_hhs']

# Handle 1-vehicle, 1-person households and modify the low-vehicle percentage
df_vehicle['perc_veh_1_hh_1'] = df_vehicle['veh_1_hh_1'] / df_vehicle['all_hhs']
df_vehicle['perc_lo_veh_mod'] = df_vehicle['perc_lo_veh'] - df_vehicle['perc_veh_1_hh_1']
df_vehicle['perc_lo_veh_mod'] = df_vehicle['perc_lo_veh_mod'].round(3)

# ----------------------- AGE DATA BY TRACT (B01001) ------------------------------
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

# Read and process age data
df_age = pd.concat([pd.read_csv(os.path.join(BASE_INPUT_FOLDER, f), skiprows=[1]).rename(columns=column_mapping_age)[list(column_mapping_age.values())]
                    for f in csv_files_age], ignore_index=True)

# Calculate youth (15-21 years) and elderly (65+ years) populations and percentages
df_age['all_youth'] = df_age[['m_15_17', 'f_15_17', 'm_18_19', 'f_18_19', 'm_20', 'f_20', 'm_21', 'f_21']].sum(axis=1)
df_age['all_elderly'] = df_age[['m_65_66', 'f_65_66', 'm_67_69', 'f_67_69', 'm_70_74', 'f_70_74',
                                'm_75_79', 'f_75_79', 'm_80_84', 'f_80_84', 'm_a_85', 'f_a_85']].sum(axis=1)

df_age['perc_youth'] = df_age['all_youth'] / df_age['total_pop']
df_age['perc_elderly'] = df_age['all_elderly'] / df_age['total_pop']

# Round percentages to three decimal places
df_age['perc_youth'] = df_age['perc_youth'].round(3)
df_age['perc_elderly'] = df_age['perc_elderly'].round(3)
df_age = df_age.drop(['total_pop'], axis=1)

# ----------------------- JOIN TRACT-LEVEL DATA ---------------------------
# Merge tract-level data (income, ethnicity, vehicle, age, language)
df_tracts = pd.merge(df_income, df_ethnicity, on='GEO_ID', how='outer')
df_tracts = pd.merge(df_tracts, df_vehicle, on='GEO_ID', how='outer')
df_tracts = pd.merge(df_tracts, df_age, on='GEO_ID', how='outer')
df_tracts = pd.merge(df_tracts, df_language, on='GEO_ID', how='outer')
df_tracts.fillna(0, inplace=True)

# Drop detailed tract-level data that is no longer needed
columns_to_drop = [
    'sub_10k', '10k_15k', '15k_20k', '20k_25k', '25k_30k', '30k_35k', '35k_40k', '40k_45k', '45k_50k', '50k_60k',
    'veh_0_hh_1', 'veh_1_hh_1', 'veh_0_hh_2', 'veh_1_hh_2', 'veh_0_hh_3', 'veh_1_hh_3', 'veh_2_hh_3',
    'veh_0_hh_4p', 'veh_1_hh_4p', 'veh_2_hh_4p',
    'm_15_17', 'm_18_19', 'm_20', 'm_21', 'm_65_66', 'm_67_69', 'm_70_74', 'm_75_79', 'm_80_84', 'm_a_85',
    'f_15_17', 'f_18_19', 'f_20', 'f_21', 'f_65_66', 'f_67_69', 'f_70_74', 'f_75_79', 'f_80_84', 'f_a_85',
    'all_youth', 'all_elderly',
    'all_hisp', 'white', 'black', 'native', 'asian', 'pac_isl', 'other', 'multi', 'minority',
    'total_lang_pop', 'spanish_engnwell', 'frenchetc_engnwell', 'germanetc_engnwell', 'slavicetc_engnwell',
    'indoeuroetc_engnwell', 'korean_engnwell', 'chineseetc_engnwell', 'vietnamese_engnwell',
    'asiapacetc_engnwell', 'arabic_engnwell', 'otheretc_engnwell', 'all_nwell',
    'low_income',
    'all_hhs', 'veh_0_all_hh', 'veh_1_all_hh', 'all_lo_veh_hh'
]
df_tracts = df_tracts.drop(columns=columns_to_drop)

# ----------------------- JOIN BLOCK-LEVEL DATA ---------------------------
# Merge block-level data (population, household, jobs)
df_blocks = pd.merge(df_population, df_household, on='GEO_ID', how='outer')
df_blocks = pd.merge(df_blocks, df_jobs, on='GEO_ID', how='outer')

# Modify 'GEO_ID' and create synthetic tract and block IDs
df_blocks['tract_id_synth'] = df_blocks['GEO_ID'].str[9:20]
df_blocks['block_id_synth'] = df_blocks['GEO_ID'].str[9:24]
df_blocks.fillna(0, inplace=True)

# ----------------------- CREATE CLEAN TRACT ID IN TRACT-LEVEL DATA ---------------------------
df_tracts['tract_id_clean'] = df_tracts['GEO_ID'].str[9:]

# ----------------------- JOIN BLOCK AND TRACT DATA USING TRACT ID ---------------------------
df_combined = pd.merge(df_blocks, df_tracts, left_on='tract_id_synth', right_on='tract_id_clean', how='outer')
df_combined.fillna(0, inplace=True)

# ----------------------- CALCULATE BLOCK-LEVEL ESTIMATES ---------------------------
df_combined['est_low_income'] = df_combined['perc_low_income'] * df_combined['total_hh']
df_combined['est_lep'] = df_combined['perc_lep'] * df_combined['total_pop']
df_combined['est_minority'] = df_combined['perc_minority'] * df_combined['total_pop']
df_combined['est_lo_veh'] = df_combined['perc_lo_veh'] * df_combined['total_hh']
df_combined['est_lo_veh_mod'] = df_combined['perc_lo_veh_mod'] * df_combined['total_hh']
df_combined['est_youth'] = df_combined['perc_youth'] * df_combined['total_pop']
df_combined['est_elderly'] = df_combined['perc_elderly'] * df_combined['total_pop']

# Round the estimates to 3 decimal places
estimate_cols = ['est_low_income', 'est_lep', 'est_minority', 'est_lo_veh', 'est_lo_veh_mod', 'est_youth', 'est_elderly']
df_combined[estimate_cols] = df_combined[estimate_cols].round(3)

# ----------------------- FILTER BLOCK DATA TO NEEDED JURISDICTIONS ---------------------------
df_combined['GEO_ID'] = df_combined['GEO_ID_x'].str[:2] + '1' + df_combined['GEO_ID_x'].str[3:]
df_combined['tract_id_synth'] = df_combined['GEO_ID'].str[9:20]
df_combined['block_id_synth'] = df_combined['GEO_ID'].str[9:24]
df_combined['FIPS_code'] = df_combined['GEO_ID'].str[9:14]
df_filtered_blocks = df_combined[df_combined['FIPS_code'].isin(fips_to_filter)].copy()

# ----------------------- CALCULATE BLOCK-LEVEL ESTIMATES FROM TRACT PERCENTAGES ---------------------------
df_filtered_blocks['est_low_income'] = df_filtered_blocks['perc_low_income'] * df_filtered_blocks['total_hh']
df_filtered_blocks['est_lep'] = df_filtered_blocks['perc_lep'] * df_filtered_blocks['total_pop']
df_filtered_blocks['est_minority'] = df_filtered_blocks['perc_minority'] * df_filtered_blocks['total_pop']
df_filtered_blocks['est_lo_veh'] = df_filtered_blocks['perc_lo_veh'] * df_filtered_blocks['total_hh']
df_filtered_blocks['est_lo_veh_mod'] = df_filtered_blocks['perc_lo_veh_mod'] * df_filtered_blocks['total_hh']
df_filtered_blocks['est_youth'] = df_filtered_blocks['perc_youth'] * df_filtered_blocks['total_pop']
df_filtered_blocks['est_elderly'] = df_filtered_blocks['perc_elderly'] * df_filtered_blocks['total_pop']
df_filtered_blocks[estimate_cols] = df_filtered_blocks[estimate_cols].round(3)

# ------------------- CONVERT DATA TYPES TO COMPATIBLE ONES ---------------------------
for column in df_filtered_blocks.columns:
    if pd.api.types.is_extension_array_dtype(df_filtered_blocks[column]):
        df_filtered_blocks[column] = df_filtered_blocks[column].astype('float64')

# ------------------- EXPORT TO CSV AND SHAPEFILE ---------------------------
# Define paths for saving the CSV and shapefile
csv_output_path = os.path.join(BASE_OUTPUT_FOLDER, CSV_OUTPUT_FILENAME)
df_filtered_blocks.to_csv(csv_output_path, index=True)
print(f"CSV file saved to: {csv_output_path}")

# Create a dedicated folder for the shapefile
shapefile_folder = os.path.join(BASE_OUTPUT_FOLDER, SHAPEFILE_FOLDER_NAME)
os.makedirs(shapefile_folder, exist_ok=True)

# Define the shapefile output path within the new folder
shapefile_output_path = os.path.join(shapefile_folder, "va_md_dc_census_blocks.shp")

# Join the block data to the filtered shapefile on 'GEO_ID'
result_gdf = filtered_gdf.merge(df_filtered_blocks, left_on='GEOIDFQ20', right_on='GEO_ID', how='left')

# Save the result as a shapefile in the newly created folder
result_gdf.to_file(shapefile_output_path)
print(f"Shapefile saved to: {shapefile_output_path}")









