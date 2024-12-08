#!/usr/bin/env python
# coding: utf-8

# In[21]:


#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import re

# ================================
# CONFIGURATION SECTION
# ================================

# Define input and output paths
input_file = r"C:\Path\To\Your\GTFS\Data" # Replace with your file path
output_folder = r"C:\Path\To\Your\Output\Folder" # Replace with your file path
output_file = r'stop_name_suffix_errors.csv'  # Suffix check output file name
output_all_stops_file = 'all_stops_by_caps_style.csv' # Capitalization check output file name

# Exempt words that are allowed even if they're not USPS suffixes
# Replace or supplement with 2 or 3-letter words common in street names or bus stop names in your data
exempt_words = [
    "AND", "VAN", "LA", "OX", "OLD", "BAY", "FOX", "LEE", "OAK", "ELM", "GUM", "MAR", "THE", "RED", "FOX", "OWL"
]
exempt_words_set = set(word.upper() for word in exempt_words)

# Approved USPS abbreviations
usps_abbreviations = [
    "ALY", "ANX", "ARC", "AVE", "BYU", "BCH", "BND", "BLF", "BLFS", "BTM", "BLVD", "BR", "BRG", "BRK", "BRKS",
    "BG", "BGS", "BYP", "CP", "CYN", "CPE", "CSWY", "CTR", "CTRS", "CIR", "CIRS", "CLF", "CLFS", "CLB", "CMN",
    "CMNS", "COR", "CORS", "CRSE", "CT", "CTS", "CV", "CVS", "CRK", "CRES", "CRST", "XING", "XRD", "XRDS", "CURV",
    "DL", "DM", "DV", "DR", "DRS", "EST", "ESTS", "EXPY", "EXT", "EXTS", "FALL", "FLS", "FRY", "FLD", "FLDS",
    "FLT", "FLTS", "FRD", "FRDS", "FRST", "FRG", "FRGS", "FRK", "FRKS", "FT", "FWY", "GDN", "GDNS", "GTWY", "GLN",
    "GLNS", "GRN", "GRNS", "GRV", "GRVS", "HBR", "HBRS", "HVN", "HTS", "HWY", "HL", "HLS", "HOLW", "INLT", "IS",
    "ISS", "ISLE", "JCT", "JCTS", "KY", "KYS", "KNL", "KNLS", "LK", "LKS", "LAND", "LNDG", "LN", "LGT", "LGTS",
    "LF", "LCK", "LCKS", "LDG", "LOOP", "MALL", "MNR", "MNRS", "MDW", "MDWS", "MEWS", "ML", "MLS", "MSN", "MTWY",
    "MT", "MTN", "MTNS", "NCK", "ORCH", "OVAL", "OPAS", "PARK", "PKWY", "PASS", "PSGE", "PATH",
    "PIKE", "PNE", "PNES", "PL", "PLN", "PLNS", "PLZ", "PT", "PTS", "PRT", "PRTS", "PR", "RADL", "RAMP", "RNCH",
    "RPD", "RPDS", "RST", "RDG", "RDGS", "RIV", "RD", "RDS", "RTE", "ROW", "RUE", "RUN", "SHL", "SHLS", "SHR",
    "SHRS", "SKWY", "SPG", "SPGS", "SPUR", "SQ", "SQS", "STA", "STRA", "STRM", "ST", "STS", "SMT", "TER",
    "TRWY", "TRCE", "TRAK", "TRFY", "TRL", "TRLR", "TUNL", "TPKE", "UPAS", "UN", "UNS", "VLY", "VLYS", "VIA",
    "VW", "VWS", "VLG", "VLGS", "VL", "VIS", "WALK", "WALL", "WAY", "WAYS", "WL", "WLS"
]

# Convert USPS abbreviations to uppercase and strip whitespace
usps_abbreviations_set = set(mod.upper().strip() for mod in usps_abbreviations)

# Combined valid short words (USPS suffixes + exempt words)
valid_short_words_set = usps_abbreviations_set.union(exempt_words_set)

# ================================
# END CONFIGURATION SECTION
# ================================

def check_capitalization(stop_name):
    """Check the capitalization scheme of the stop name."""
    stop_name_lower = stop_name.lower()
    stop_name_upper = stop_name.upper()

    # Check for all lowercase
    if stop_name == stop_name_lower:
        return 'ALL_LOWERCASE'

    # Check for all uppercase
    elif stop_name == stop_name_upper:
        return 'ALL_UPPERCASE'

    # Check for proper title case with exceptions for small words
    exceptions = {"and", "or", "the", "in", "at", "by", "to", "for", "of", "on", "as", "a", "an", "but"}
    title_case_words = stop_name.title().split()
    for i in range(1, len(title_case_words)):  # Start from index 1 to keep the first word capitalized
        if title_case_words[i].lower() in exceptions:
            title_case_words[i] = title_case_words[i].lower()  # Keep small words lowercase

    title_case_normalized = " ".join(title_case_words)
    if stop_name == title_case_normalized:
        return 'PROPER_TITLE_CASE'

    # Check for first letter capitalization
    elif stop_name[0].isupper() and stop_name[1:].islower():
        return 'FIRST_LETTER_CAPITALIZED'

    # Otherwise, it's mixed case
    else:
        return 'MIXED_CASE'

def check_usps_suffix(stop_name):
    """Check if the stop name ends with a valid USPS suffix, only if the suffix is a short word."""
    stop_name_parts = stop_name.split()
    if not stop_name_parts:
        return False, "Empty stop name"
    last_part = stop_name_parts[-1].upper()
    if len(last_part) in [2, 3]:  # Only consider short words
        if last_part not in usps_abbreviations_set:
            return False, f"Invalid suffix: {last_part}"
    # If the last word is longer than 3 letters, do not flag it as an error
    return True, ""

def find_invalid_short_words(stop_name):
    """Find two or three-letter words in the stop name not in USPS abbreviations or exempt words."""
    words = stop_name.split()
    short_words = [word.upper() for word in words if len(word) in [2, 3]]
    invalid_words = [word for word in short_words if word not in valid_short_words_set]
    return invalid_words

def main():
    # Set input and output paths
    input_file_path = input_file
    output_folder_path = output_folder
    output_file_path = os.path.join(output_folder_path, output_file)
    output_all_stops_file_path = os.path.join(output_folder_path, output_all_stops_file)  # For all stops file with scheme

    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)

    # Load stops data from GTFS stops.txt
    stops_df = pd.read_csv(input_file_path, dtype=str)  # Read all columns as strings

    # Ensure the required columns are present
    required_columns_stops = ['stop_id', 'stop_name']
    missing_columns = [col for col in required_columns_stops if col not in stops_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in stops.txt: {', '.join(missing_columns)}")

    # Initialize counters for capitalization schemes
    all_lowercase_count = 0
    all_uppercase_count = 0
    proper_title_case_count = 0
    first_letter_cap_count = 0
    mixed_case_count = 0

    # Initialize lists to store errors
    suffix_errors = []
    short_word_errors = []
    unique_invalid_short_words = set()

    # Loop through stop names and check capitalization and USPS suffixes
    for _, row in stops_df.iterrows():
        stop_name = row['stop_name'].strip()

        # Check capitalization scheme
        capitalization_scheme = check_capitalization(stop_name)
        if capitalization_scheme == 'ALL_LOWERCASE':
            all_lowercase_count += 1
        elif capitalization_scheme == 'ALL_UPPERCASE':
            all_uppercase_count += 1
        elif capitalization_scheme == 'PROPER_TITLE_CASE':
            proper_title_case_count += 1
        elif capitalization_scheme == 'FIRST_LETTER_CAPITALIZED':
            first_letter_cap_count += 1
        else:
            mixed_case_count += 1

        # Check USPS suffixes (only short words)
        is_suffix_valid, suffix_message = check_usps_suffix(stop_name)
        if not is_suffix_valid:
            suffix_errors.append({
                'stop_id': row['stop_id'],
                'stop_name': stop_name,
                'error': suffix_message
            })

        # Check for invalid two or three-letter words
        invalid_short_words = find_invalid_short_words(stop_name)
        if invalid_short_words:
            short_word_errors.append({
                'stop_id': row['stop_id'],
                'stop_name': stop_name,
                'invalid_short_words': ', '.join(invalid_short_words)
            })
            unique_invalid_short_words.update(invalid_short_words)

        # Add the capitalization scheme to the DataFrame
        stops_df.at[_, 'capitalization_scheme'] = capitalization_scheme

    # Calculate the total number of stops
    total_stops = len(stops_df)

    # Calculate percentages
    all_lowercase_percent = (all_lowercase_count / total_stops) * 100
    all_uppercase_percent = (all_uppercase_count / total_stops) * 100
    proper_title_case_percent = (proper_title_case_count / total_stops) * 100
    first_letter_cap_percent = (first_letter_cap_count / total_stops) * 100
    mixed_case_percent = (mixed_case_count / total_stops) * 100

    # Print the percentages
    print(f"Percent of stops with ALL LOWERCASE: {all_lowercase_percent:.2f}%")
    print(f"Percent of stops with ALL UPPERCASE: {all_uppercase_percent:.2f}%")
    print(f"Percent of stops with PROPER TITLE CASE (Proper capitalization): {proper_title_case_percent:.2f}%")
    print(f"Percent of stops with FIRST LETTER CAPITALIZED: {first_letter_cap_percent:.2f}%")
    print(f"Percent of stops with MIXED CASE: {mixed_case_percent:.2f}%")

    # Combine all errors (only short word related errors)
    all_errors = []

    # Prepare suffix errors (only short word suffixes)
    for error in suffix_errors:
        all_errors.append({
            'stop_id': error['stop_id'],
            'stop_name': error['stop_name'],
            'error': error['error']
        })

    # Prepare short word errors
    for error in short_word_errors:
        all_errors.append({
            'stop_id': error['stop_id'],
            'stop_name': error['stop_name'],
            'error': f"Invalid short words: {error['invalid_short_words']}"
        })

    # Save the results to a CSV file
    if all_errors:
        errors_df = pd.DataFrame(all_errors)
        errors_df.to_csv(output_file_path, index=False)
        print(f"Errors found. Report saved to {output_file_path}")
    else:
        print("No errors found.")

    # Export all stops with their ids, names, and capitalization scheme
    stops_df_export = stops_df[['stop_id', 'stop_name', 'capitalization_scheme']]
    stops_df_export.to_csv(output_all_stops_file_path, index=False)
    print(f"All stops exported to {output_all_stops_file_path}")

    # Print the number and unique values of invalid short words
    total_short_word_errors = len(short_word_errors)
    print(f"Number of stops with invalid two or three-letter words: {total_short_word_errors}")
    if unique_invalid_short_words:
        print(f"Unique invalid two or three-letter words: {', '.join(sorted(unique_invalid_short_words))}")
    else:
        print("No unique invalid two or three-letter words found.")

if __name__ == "__main__":
    main()


# In[ ]:




