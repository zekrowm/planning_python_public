"""
This script processes transportation route geometries and stop locations to split routes
into segments based on stop positions. It is designed for use with transit network data
and GTFS feeds, making it suitable for tasks such as stop spacing analysis, transit
performance evaluation, and network optimization.
"""

import os
import csv
import statistics
from collections import defaultdict

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points, split, unary_union

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------
# 1) Input GTFS and route shapefile
input_folder = r"C:\Users\Your\GTFS_Data_Folder" # Replace with your folder path
route_shapefile = r"C:\Users\Your\bus_routes.shp" # Replace with your file path

# 2) Base output folder (ALL outputs go here)
base_output_folder = r"C:\Users\Your\Output_Folder" # Replace with your folder path
os.makedirs(base_output_folder, exist_ok=True)

# 3) Route short name filter. If empty list, process all route short names in shapefile.
route_filter = ['101', '102']  # or [] to process all

# 4) Coordinate system (well-known ID) for Northern Virginia: EPSG:2283 (NAD83 / Virginia North)
#    We assume this has FEET as its linear unit. If it's actually meters, multiply by 3.28084 below.
target_crs = "EPSG:2283" # Replace with with an EPSG code appropriate for your study area

# 5) Field name configurations
shp_field_original_route_num = 'ROUTE_NUMB' # Replace with the short route name column in your route_shapefile data
shp_field_new_route_short = 'route_shor'
shp_field_new_route_shrt = 'ROUTE_SHRT'  # no spaces


# ----------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------

def read_routes_from_shapefile(gdf, route_filter, orig_route_field):
    """
    Reads unique routes from the shapefile and returns:
      (1) a list of route_short_names that match the filter,
      (2) a dict mapping route_short_name -> route_shrt (no spaces).
    """
    if not route_filter:
        route_values = gdf[orig_route_field].unique().tolist()
        route_values = [str(r).strip() for r in route_values if isinstance(r, (str, int, float))]
    else:
        route_values = []
        for r in gdf[orig_route_field].unique():
            r_stripped = str(r).strip()
            if r_stripped in route_filter:
                route_values.append(r_stripped)
    route_dict = {r: r.replace(" ", "") for r in route_values}
    return sorted(route_values), route_dict

def add_route_fields(gdf, orig_field, new_field_short, new_field_shrt):
    """
    Adds/updates route_short_name (new_field_short) and ROUTE_SHRT (new_field_shrt)
    columns in the GeoDataFrame based on the original route field.
    """
    if new_field_short not in gdf.columns:
        gdf[new_field_short] = None
    if new_field_shrt not in gdf.columns:
        gdf[new_field_shrt] = None

    for idx, row in gdf.iterrows():
        route_num = str(row[orig_field]).strip()
        route_shrt = route_num.replace(" ", "")
        gdf.at[idx, new_field_short] = route_num
        gdf.at[idx, new_field_shrt] = route_shrt
    return gdf


def get_stops_for_route_and_direction(gtfs_folder, route_short_name):
    """
    Reads GTFS tables and returns a dictionary like:
       {
         '0': DataFrame of stops for direction=0,
         '1': DataFrame of stops for direction=1,
         ...
       }
    If a route has multiple directions, each direction is returned separately.
    """
    routes_path = os.path.join(gtfs_folder, "routes.txt")
    trips_path = os.path.join(gtfs_folder, "trips.txt")
    stop_times_path = os.path.join(gtfs_folder, "stop_times.txt")
    stops_path = os.path.join(gtfs_folder, "stops.txt")

    # Read CSVs
    routes_df = pd.read_csv(routes_path, dtype=str)
    trips_df = pd.read_csv(trips_path, dtype=str)
    stop_times_df = pd.read_csv(stop_times_path, dtype=str)
    stops_df = pd.read_csv(stops_path, dtype=str)

    # Filter routes to the matching route_short_name
    filtered_routes = routes_df[routes_df['route_short_name'] == route_short_name].copy()
    if filtered_routes.empty:
        return {}

    # Grab all route_id values that match this short name
    route_ids = filtered_routes['route_id'].unique().tolist()

    # Filter trips to these route_ids
    trips_for_route = trips_df[trips_df['route_id'].isin(route_ids)]
    if trips_for_route.empty:
        return {}

    directions_dict = {}
    for direction_val, direction_trips in trips_for_route.groupby('direction_id'):
        trip_ids = direction_trips['trip_id'].unique().tolist()
        stop_times_for_dir = stop_times_df[stop_times_df['trip_id'].isin(trip_ids)]
        if stop_times_for_dir.empty:
            directions_dict[direction_val] = pd.DataFrame(columns=stops_df.columns)
            continue
        stop_ids = stop_times_for_dir['stop_id'].unique().tolist()
        stops_for_dir = stops_df[stops_df['stop_id'].isin(stop_ids)].copy()
        directions_dict[direction_val] = stops_for_dir

    return directions_dict


def build_stops_geodataframe(stops_df, wgs84_crs="EPSG:4326"):
    """
    Convert a DataFrame of stops (with columns stop_lat, stop_lon) into a GeoDataFrame.
    Drops rows with invalid lat/lon.
    """
    if stops_df.empty:
        return gpd.GeoDataFrame(stops_df, geometry=[], crs=wgs84_crs)

    stops_df["stop_lat"] = pd.to_numeric(stops_df["stop_lat"], errors='coerce')
    stops_df["stop_lon"] = pd.to_numeric(stops_df["stop_lon"], errors='coerce')
    stops_df = stops_df.dropna(subset=["stop_lat", "stop_lon"])

    geometry = [Point(xy) for xy in zip(stops_df["stop_lon"], stops_df["stop_lat"])]
    gdf_stops = gpd.GeoDataFrame(stops_df, geometry=geometry, crs=wgs84_crs)
    return gdf_stops


def pieces_union_all(pieces, crs):
    """
    Combines a list of LineString objects into a single geometry using 'union_all()' if available.
    Falls back to shapely.ops.unary_union() if 'union_all()' is not available.
    """
    geo_series = gpd.GeoSeries(pieces, crs=crs)

    # If Shapely >= 2.0, union_all() is the recommended approach.
    if hasattr(geo_series, 'union_all'):
        # Returns a GeoSeries with one geometry, so we take .iloc[0].
        return geo_series.union_all().iloc[0]
    else:
        # Fallback for older Shapely
        return unary_union(geo_series)


def split_line_at_points(line, points, buffer_radius=1e-6):
    """
    Splits a single Shapely LineString by a collection of (snapped) points.
    Applies a tiny buffer to prevent floating-point issues.
    Returns a list of LineStrings.
    """
    for pt in points:
        line = split(line, pt.buffer(buffer_radius))
        pieces = []

        if line.geom_type == "MultiLineString":
            for segment in line.geoms:
                pieces.append(segment)
        elif line.geom_type == "LineString":
            pieces.append(line)
        else:
            for subgeom in line.geoms:
                if subgeom.geom_type == "LineString":
                    pieces.append(subgeom)

        try:
            line = pieces_union_all(pieces, line.crs)
        except AttributeError:
            # Fallback in case union_all not available
            line = unary_union(gpd.GeoSeries(pieces))

    if line.geom_type == "MultiLineString":
        return list(line.geoms)
    elif line.geom_type == "LineString":
        return [line]
    else:
        segments = []
        for g in line.geoms:
            if g.geom_type == "LineString":
                segments.append(g)
        return segments


# ----------------------------------------------------------------------
# MAIN SCRIPT
# ----------------------------------------------------------------------
def main():
    # 1) Read the route shapefile (all routes)
    gdf_routes = gpd.read_file(route_shapefile)

    # 2) Read unique route short names from shapefile & filter
    route_list, route_dict = read_routes_from_shapefile(
        gdf_routes, route_filter, shp_field_original_route_num
    )
    print("Routes to process:", route_list)

    # Warn if some filtered routes are not present in the shapefile
    if route_filter:
        missing_routes = set(route_filter) - set(route_list)
        for mr in missing_routes:
            print(f"WARNING: Route '{mr}' specified in filter not found in shapefile.")

    # 3) Add 'route_short_name' & 'ROUTE_SHRT' columns to the route GDF
    gdf_routes = add_route_fields(
        gdf_routes,
        shp_field_original_route_num,
        shp_field_new_route_short,
        shp_field_new_route_shrt
    )

    # 4) Project the route shapefile to target CRS
    gdf_routes_prj = gdf_routes.to_crs(target_crs)

    # 5) Write out the projected original route shapes for visual inspection (one shapefile with all routes)
    shapes_shp = os.path.join(base_output_folder, "all_routes_projected.shp")
    gdf_routes_prj.to_file(shapes_shp, driver="ESRI Shapefile")
    print(f"All route shapes projected to Shapefile: {shapes_shp}")

    # Prepare CSV data (we'll add a DirectionID column)
    csv_rows = [["Route", "DirectionID", "SegmentOrder", "SegmentLength"]]

    # For snapping lines
    snap_lines_data = []

    # For summary stats: dict key = (route, direction_id), value = list of segment lengths
    route_segments_summary = defaultdict(list)

    # ------------------------------------------------------------------
    # 6) PROCESS EACH ROUTE
    # ------------------------------------------------------------------
    for route_sn in route_list:
        print(f"\nProcessing route: {route_sn}")

        # Filter route lines from gdf_routes_prj by 'ROUTE_NUMB'
        route_lines = gdf_routes_prj[gdf_routes_prj[shp_field_original_route_num] == route_sn].copy()
        if route_lines.empty:
            print(f"No geometry found for route {route_sn} in the shapefile.")
            continue

        # Create the base folder for this route
        route_output_dir = os.path.join(base_output_folder, route_sn.replace(" ", "_"))
        os.makedirs(route_output_dir, exist_ok=True)

        # ------------------------------------------------------------------
        # A) GRAB STOPS FROM GTFS FOR THIS ROUTE, SEPARATED BY DIRECTION
        # ------------------------------------------------------------------
        stops_by_dir = get_stops_for_route_and_direction(input_folder, route_sn)
        if not stops_by_dir:
            print(f"No stops found in GTFS for route_short_name='{route_sn}'. Skipping.")
            continue

        # For each direction_id in stops_by_dir
        for direction_val, stops_df_for_dir in stops_by_dir.items():
            dir_str = str(direction_val).strip() if pd.notna(direction_val) else "unknown"
            print(f"  Direction: {dir_str}")

            if stops_df_for_dir.empty:
                print(f"  No stops for direction={dir_str}. Skipping.")
                continue

            # Convert DataFrame -> GeoDataFrame
            gdf_stops_for_dir = build_stops_geodataframe(stops_df_for_dir, wgs84_crs="EPSG:4326")
            if gdf_stops_for_dir.empty:
                print(f"  No valid lat/lon for direction={dir_str}. Skipping.")
                continue

            # Project them to the target CRS
            gdf_stops_for_dir = gdf_stops_for_dir.to_crs(target_crs)

            # Create a sub-folder for this route+direction
            dir_output_dir = os.path.join(route_output_dir, f"direction_{dir_str}")
            os.makedirs(dir_output_dir, exist_ok=True)

            # Export the GTFS stops (un-snapped) for visual validation
            raw_stops_shp = os.path.join(dir_output_dir, f"{route_sn}_dir{dir_str}_raw_stops.shp")
            gdf_stops_for_dir.to_file(raw_stops_shp, driver="ESRI Shapefile")
            print(f"    Exported raw GTFS stops for route {route_sn}, dir={dir_str} to: {raw_stops_shp}")

            # ------------------------------------------------------------------
            # B) SNAP STOPS TO ROUTE LINES
            # ------------------------------------------------------------------
            snapped_points = []
            for _, stop_row in gdf_stops_for_dir.iterrows():
                stop_pt = stop_row.geometry

                # Find the line in route_lines that is closest to this stop
                nearest_geom = None
                min_dist = float("inf")
                for _, line_row in route_lines.iterrows():
                    line = line_row.geometry
                    dist = stop_pt.distance(line)
                    if dist < min_dist:
                        min_dist = dist
                        nearest_geom = line

                if nearest_geom is None:
                    print(f"WARNING: No nearest geometry found for stop {stop_row.get('stop_id', 'Unknown')}.")
                    continue

                # Snap the stop to the line
                snapped_pt_on_line, _ = nearest_points(nearest_geom, stop_pt)
                snapped_points.append(snapped_pt_on_line)

                # Store a line from original -> snapped for visual check
                snap_lines_data.append({
                    "Route": route_sn,
                    "DirectionID": dir_str,
                    "StopID": stop_row.get("stop_id", ""),
                    "Distance": min_dist,
                    "geometry": LineString([stop_pt, snapped_pt_on_line])
                })

            # Build a GeoDataFrame for the snapped stops
            snapped_stops_gdf = gdf_stops_for_dir.copy()
            snapped_stops_gdf = snapped_stops_gdf.iloc[:len(snapped_points)].copy()
            snapped_stops_gdf["geometry"] = snapped_points

            # Write the snapped stops to a shapefile in this route+direction folder
            snapped_stops_shp = os.path.join(dir_output_dir, f"{route_sn}_dir{dir_str}_snapped_stops.shp")
            snapped_stops_gdf.to_file(snapped_stops_shp, driver="ESRI Shapefile")
            print(f"    Snapped stops shapefile created at: {snapped_stops_shp}")

            # ------------------------------------------------------------------
            # C) SPLIT THE ROUTE GEOMETRY AT THESE SNAPPED STOP LOCATIONS
            # ------------------------------------------------------------------
            route_segments = []
            if snapped_stops_gdf.empty:
                print(f"    No snapped stops to split route {route_sn}, dir={dir_str}.")
                continue

            for route_geom in route_lines.geometry:
                try:
                    route_segments.extend(
                        split_line_at_points(route_geom, list(snapped_stops_gdf.geometry))
                    )
                except Exception as e:
                    print(f"    ERROR while splitting route {route_sn}, direction={dir_str}: {e}")
                    continue

            # Remove near-zero-length segments
            MIN_LENGTH_FEET = 0.01  # or pick your own
            route_segments = [seg for seg in route_segments if seg.length > MIN_LENGTH_FEET]

            # ------------------------------------------------------------------
            # D) CALCULATE SEGMENT LENGTHS & WRITE TO CSV
            # ------------------------------------------------------------------
            seg_lengths = []
            for i, segment in enumerate(route_segments, start=1):
                length_ft = segment.length  # EPSG:2283 should be in feet
                seg_lengths.append(length_ft)
                csv_rows.append([route_sn, dir_str, i, length_ft])

            route_segments_summary[(route_sn, dir_str)].extend(seg_lengths)

            if seg_lengths:
                mn = min(seg_lengths)
                mx = max(seg_lengths)
                avg = sum(seg_lengths) / len(seg_lengths)
                med = statistics.median(seg_lengths)
                print(f"    Route {route_sn}, dir={dir_str} -> "
                      f"min={mn:.2f}ft, max={mx:.2f}ft, avg={avg:.2f}ft, med={med:.2f}ft")

            # ------------------------------------------------------------------
            # E) EXPORT SPLIT SEGMENTS TO SHAPEFILE WITH LENGTH
            # ------------------------------------------------------------------
            gdf_segments = gpd.GeoDataFrame(
                {
                    "SegmentID": list(range(1, len(route_segments) + 1)),
                    "Length_ft": [round(seg.length, 2) for seg in route_segments]
                },
                geometry=route_segments,
                crs=route_lines.crs
            )
            route_split_shp = os.path.join(dir_output_dir, f"{route_sn}_dir{dir_str}_split_segments.shp")
            gdf_segments.to_file(route_split_shp, driver="ESRI Shapefile")
            print(f"    Copied split segments for route {route_sn}, dir={dir_str} to {route_split_shp}")

    # ------------------------------------------------------------------
    # 7) WRITE SEGMENT-LEVEL CSV (ALL ROUTES & DIRECTIONS)
    # ------------------------------------------------------------------
    segment_csv_path = os.path.join(base_output_folder, "segment_spacing.csv")
    with open(segment_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(csv_rows)
    print(f"\nSegment spacing CSV written to: {segment_csv_path}")

    # ------------------------------------------------------------------
    # 8) WRITE SNAP LINES SHP (ORIGINAL -> SNAPPED) - FOR ALL ROUTES
    # ------------------------------------------------------------------
    if snap_lines_data:
        snap_lines_gdf = gpd.GeoDataFrame(snap_lines_data, geometry='geometry', crs=gdf_routes_prj.crs)
        snap_lines_gdf["SnapLen_ft"] = snap_lines_gdf["Distance"].astype(float).round(2)

        snap_lines_shp = os.path.join(base_output_folder, "snap_lines_all_routes.shp")
        snap_lines_gdf.to_file(snap_lines_shp, driver="ESRI Shapefile")
        print(f"Snap lines shapefile created at: {snap_lines_shp}")

    # ------------------------------------------------------------------
    # 9) WRITE SUMMARY XLSX
    # ------------------------------------------------------------------
    summary_data = [("Route", "DirectionID", "Min (ft)", "Max (ft)", "Mean (ft)", "Median (ft)")]
    for (r, d), lengths in route_segments_summary.items():
        if not lengths:
            summary_data.append((r, d, None, None, None, None))
            continue
        mn = min(lengths)
        mx = max(lengths)
        avg = sum(lengths) / len(lengths)
        med = statistics.median(lengths)
        summary_data.append((r, d, mn, mx, avg, med))

    summary_xlsx_path = os.path.join(base_output_folder, "spacing_summary.xlsx")
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Spacing Summary"

        for row_idx, row_val in enumerate(summary_data, start=1):
            for col_idx, col_val in enumerate(row_val, start=1):
                ws.cell(row=row_idx, column=col_idx, value=col_val)

        wb.save(summary_xlsx_path)
        print(f"Summary XLSX written to: {summary_xlsx_path}")
    except ImportError:
        print("openpyxl not installed. Could not write XLSX. Install openpyxl or export CSV.")


if __name__ == "__main__":
    main()
