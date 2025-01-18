#!/usr/bin/env python
# coding: utf-8

"""
This script optimizes bus bay assignments by analyzing GTFS data directly
and uses a more sophisticated minute-by-minute conflict logic to determine
bus/block statuses, such as laying over and off-service periods.
"""

import os
import math
import re
import pandas as pd
import pulp
from collections import defaultdict

###############################################################################
# CONFIGURATION
###############################################################################

CONFIG = {
    "gtfs_folder": r"C:\Users\zach\Desktop\Zach\python_stuff\projects\bus_bay_optimization_2025_01_16\GTFS Apr 2025",
    "whitelisted_service_id": '1',
    "stops_of_interest": ['1001', '1002'],
    "num_bays_per_stop": {
        "1001": 1,
        "1002": 1
    },
    "output_folder": r"C:\Your\Folder\Path\for\Output",
    # We'll create a special comparison file
    "comparison_output_filename": "BayAssignment_Comparison.xlsx",
    # New Configuration Parameters
    "extended_offservice_threshold": 60  # in minutes
}

LAYOVER_THRESHOLD = 15  # in minutes

###############################################################################
# SNIPPET-STYLE HELPER FUNCTIONS
###############################################################################

def time_to_seconds(t):
    """Convert HH:MM:SS time format to total seconds, including times >= 24:00:00."""
    parts = t.split(":")
    h, m, s = map(int, parts)
    return h * 3600 + m * 60 + s

def get_trip_ranges_and_ends(block_segments):
    """Return (trip_id, trip_start, trip_end, route_short_name, direction_id, start_stop, end_stop)."""
    trips_info = []
    for tid in block_segments['trip_id'].unique():
        tsub = block_segments[block_segments['trip_id'] == tid].sort_values('arrival_seconds')
        trip_start = tsub['arrival_seconds'].min()
        trip_end   = tsub['departure_seconds'].max()
        route_short_name = tsub['route_short_name'].iloc[0]
        direction_id = tsub['direction_id'].iloc[0]
        start_stop = tsub.iloc[0]['stop_id']
        end_stop   = tsub.iloc[-1]['stop_id']
        trips_info.append((tid, trip_start, trip_end, route_short_name, direction_id, start_stop, end_stop))
    trips_info.sort(key=lambda x: x[1])
    return trips_info

def get_minute_status_location_complex(
    minute: int, 
    block_segments: pd.DataFrame, 
    trips_info: list, 
    layover_threshold: int = LAYOVER_THRESHOLD, 
    extended_offservice_threshold: int = CONFIG["extended_offservice_threshold"]
):
    """
    Logic logic for determining the bus's status and location at a given minute.
    
    Inputs:
    -------
    minute : int
        Minute of the day (00:00 = minute 0).
    block_segments : pd.DataFrame
        Rows for this block from stop_times (already merged with route info, etc.).
    trips_info : list
        A list of (trip_id, trip_start, trip_end, route_short_name, direction_id, start_stop, end_stop),
        sorted by trip_start.
    layover_threshold : int
        The (short) layover threshold in minutes to distinguish 'laying over' vs. 'running route'.
    extended_offservice_threshold : int
        A longer threshold in minutes indicating the bus is truly off-service 
        (e.g., if the gap between consecutive trips is large).
    
    Returns:
    --------
    (status, location, route_short_name, direction_id, stop_id)
    
    Possible statuses:
      - "dwelling at stop"   => The bus is physically at a stop during scheduled dwell time
      - "running route"      => The bus is traveling between stops
      - "laying over"        => The bus is at the last stop of the previous trip, within layover threshold
      - "off-service"        => The bus is not in active use (gap > extended_offservice_threshold)
      - "inactive"           => The bus is outside its entire operating window for the day
    location:
      - If "dwelling at stop" or "laying over", location = the stop_id
      - If "running route", location = "traveling between stops"
      - If "off-service" or "inactive", location = "inactive"
    """
    current_sec = minute * 60
    
    if not trips_info:
        # No trips in this block => always inactive
        return ("inactive", "inactive", "", "", "")
    
    # Identify earliest start and latest end among all trips in this block
    earliest_start = min(t[1] for t in trips_info)
    latest_end     = max(t[2] for t in trips_info)
    
    # If we’re before the first trip or after the last trip
    if current_sec < earliest_start or current_sec > latest_end:
        return ("inactive", "inactive", "", "", "")
    
    # Check if we are in the window of a specific trip
    active_trip_idx = None
    for i, (tid, tstart, tend, rname, dirid, start_stp, end_stp) in enumerate(trips_info):
        if tstart <= current_sec <= tend:
            active_trip_idx = i
            break
    
    if active_trip_idx is not None:
        # We found an active trip whose time range contains current_sec
        (tid, tstart, tend, rname, dirid, start_stp, end_stp) = trips_info[active_trip_idx]
        tsub = block_segments[block_segments['trip_id'] == tid].sort_values('arrival_seconds')
        
        # Step through each stop in this trip
        for i in range(len(tsub)):
            row = tsub.iloc[i]
            arr_sec = row['arrival_seconds']
            dep_sec = row['departure_seconds']
            nstp    = row['next_stop_id']
            narr    = row['next_arrival_seconds']
            
            # Dwelling at the current stop if current_sec is between arr_sec and dep_sec
            if arr_sec <= current_sec <= dep_sec:
                return ("dwelling at stop", row['stop_id'], rname, dirid, row['stop_id'])
            
            # If we have a next stop time, check if we’re traveling
            if pd.notnull(narr):
                # Convert narr to an int if needed
                narr_sec = int(narr)
                
                if dep_sec < current_sec < narr_sec:
                    # traveling or possibly a short layover
                    # If the next stop is the same as the current stop => potential layover
                    if nstp == row['stop_id']:
                        gap = narr_sec - dep_sec
                        if gap > layover_threshold * 60:
                            return ("laying over", row['stop_id'], rname, dirid, row['stop_id'])
                        else:
                            return ("running route", "traveling between stops", rname, dirid, "")
                    else:
                        return ("running route", "traveling between stops", rname, dirid, "")
        
        # If we reach here, we might be beyond the last stop's departure_seconds 
        # but still within tstart–tend (which can happen if there's slack time at the end).
        # We can treat that as either "laying over" or "off-service" at the final stop.
        return ("laying over", end_stp, rname, dirid, end_stp)
    
    else:
        # We’re between trips within this block. Determine if it’s a short layover or extended off-service.
        # 1) Find the trip that ended last
        # 2) Find the trip that will start next
        # 3) Evaluate the gap
        prev_trip = None
        next_trip = None
        
        for i, (tid, tstart, tend, rname, dirid, start_stp, end_stp) in enumerate(trips_info):
            if tend < current_sec:
                prev_trip = (tid, tstart, tend, rname, dirid, start_stp, end_stp)
            if current_sec < tstart and next_trip is None:
                next_trip = (tid, tstart, tend, rname, dirid, start_stp, end_stp)
                break
        
        # If there is no previous trip, that means we haven't reached the first trip's start yet
        if not prev_trip:
            return ("inactive", "inactive", "", "", "")
        
        (ptid, ptstart, ptend, prname, pdirid, pstart_stp, pend_stp) = prev_trip
        gap_to_next_trip = None
        
        if next_trip:
            (ntid, ntstart, ntend, nrname, ndirid, nstart_stp, nend_stp) = next_trip
            gap_to_next_trip = ntstart - ptend
        
        # If there's a next trip, check how large the gap is
        if gap_to_next_trip is not None:
            if gap_to_next_trip <= layover_threshold * 60:
                # Bus is presumably laying over at the previous trip's end stop
                return ("laying over", pend_stp, prname, pdirid, pend_stp)
            elif gap_to_next_trip <= extended_offservice_threshold * 60:
                # Gap is bigger than a “short layover” but maybe not truly off the property
                # Here, we treat it as "off-service"
                return ("off-service", "inactive", "", "", "")
            else:
                # Very large gap
                return ("off-service", "inactive", "", "", "")
        else:
            # No next trip => we are after the last trip, but we’re inside earliest_start..latest_end
            # This can happen if the last trip ended earlier than the block’s "latest_end" 
            # but GTFS didn’t schedule any more trips in the block.
            # Usually treat as off-service or layover at final stop:
            return ("off-service", "inactive", "", "", "")

###############################################################################
# LOADING & PROCESSING GTFS
###############################################################################

def load_gtfs_data(gtfs_folder):
    """Load essential GTFS CSVs into DataFrames."""
    required_files = ["trips.txt", "stop_times.txt", "routes.txt", "stops.txt", "calendar.txt"]
    data = {}
    for file in required_files:
        path = os.path.join(gtfs_folder, file)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required GTFS file not found: {path}")
        print(f"Loading {file}...")
        data[file.split('.')[0]] = pd.read_csv(path, dtype=str)
    return data

def process_gtfs_data(gtfs_data, whitelisted_service_id, stops_of_interest):
    """
    Build a route->set-of-minutes mapping using minute-by-minute logic.
    If bus is "dwelling" or "laying over" at stops_of_interest in minute M,
    we mark that minute in route_to_minutes[route_short_name].
    """

    # 1. Filter trips
    trips = gtfs_data['trips']
    trips = trips[trips['service_id'] == whitelisted_service_id]
    print(f"Filtered trips to service_id '{whitelisted_service_id}': {len(trips)} trips")

    # 2. Merge routes into trips
    routes = gtfs_data['routes'][['route_id', 'route_short_name']]
    trips = trips.merge(routes, on='route_id', how='left')

    # 3. Merge stop_times
    stop_times = gtfs_data['stop_times']
    stop_times = stop_times[stop_times['trip_id'].isin(trips['trip_id'])]
    stop_times = stop_times.merge(
        trips[['trip_id', 'route_id', 'route_short_name', 'block_id', 'direction_id']],
        on='trip_id', how='left'
    )

    # 4. Convert arrival/departure times
    stop_times['arrival_seconds']   = stop_times['arrival_time'].apply(time_to_seconds)
    stop_times['departure_seconds'] = stop_times['departure_time'].apply(time_to_seconds)

    # 5. Next stops
    stop_times.sort_values(['block_id', 'trip_id', 'stop_sequence'], inplace=True)
    stop_times['next_stop_id'] = stop_times.groupby('trip_id')['stop_id'].shift(-1)
    stop_times['next_arrival_seconds'] = stop_times.groupby('trip_id')['arrival_seconds'].shift(-1)
    stop_times['next_departure_seconds'] = stop_times.groupby('trip_id')['departure_seconds'].shift(-1)

    # 6. Build route->minutes
    route_to_minutes = defaultdict(set)
    blocks = stop_times['block_id'].unique()
    print(f"Found {len(blocks)} unique blocks.")

    for blk in blocks:
        block_segments = stop_times[stop_times['block_id'] == blk].copy()
        if block_segments.empty:
            continue
        # Gather trip ranges
        trips_info = get_trip_ranges_and_ends(block_segments)
        if not trips_info:
            continue

        minSec = min(t[1] for t in trips_info)
        maxSec = max(t[2] for t in trips_info)
        start_min = minSec // 60
        end_min   = maxSec // 60

        # Minute-by-minute
        for minute in range(start_min, end_min + 1):
            status, location, rname, dirid, stop_id = get_minute_status_location_complex(
                minute,
                block_segments,
                trips_info,
                layover_threshold=LAYOVER_THRESHOLD,
                extended_offservice_threshold=CONFIG["extended_offservice_threshold"]
            )
            # If physically at a stop of interest, record this minute
            if status in ["dwelling at stop", "laying over"] and location in stops_of_interest:
                route_to_minutes[rname].add(minute)

    print(f"Built route->minutes map using complex logic.")
    return dict(route_to_minutes)

###############################################################################
# CONFLICT & ASSIGNMENT LOGIC
###############################################################################

def build_default_assignments(route_to_minutes, bay_labels):
    """
    A trivial "default" assignment, e.g. round-robin each route among the available bays.
    """
    default_assignment = {}
    route_ids = sorted(route_to_minutes.keys())
    i = 0
    for r in route_ids:
        default_assignment[r] = bay_labels[i % len(bay_labels)]
        i += 1
    return default_assignment

def build_and_solve_ilp(route_to_minutes, bay_labels, stop_ids):
    """
    Standard ILP approach: minimize total conflicts from a bay perspective.
    We'll still want to compute route-perspective conflict separately.
    """
    print(f"\nOptimizing Bay Assignments for Stops: {stop_ids}")
    model = pulp.LpProblem("BusBayAssignment", pulp.LpMinimize)

    route_ids = sorted(route_to_minutes.keys())

    # x_{r,b} = 1 if route r assigned to bay b
    x = {}
    for r in route_ids:
        for b in bay_labels:
            sr = re.sub(r'[^A-Za-z0-9]', '_', r)
            sb = re.sub(r'[^A-Za-z0-9]', '_', b)
            x[(r, b)] = pulp.LpVariable(f"x_{sr}_{sb}", cat=pulp.LpBinary)

    # Each route must be assigned to exactly one bay
    for r in route_ids:
        model += (
            pulp.lpSum([x[(r, b)] for b in bay_labels]) == 1,
            f"OneBayPerRoute_{r}"
        )

    # z_{b,m} = how many overlapping routes are present at bay b, minus 1
    z = {}
    all_minutes = sorted({m for mins in route_to_minutes.values() for m in mins})
    for b in bay_labels:
        for m in all_minutes:
            active_routes = [r for r in route_ids if m in route_to_minutes[r]]
            if active_routes:
                var_name = f"z_{re.sub(r'[^A-Za-z0-9]', '_', b)}_{m}"
                z[(b, m)] = pulp.LpVariable(var_name, lowBound=0, cat=pulp.LpInteger)

                model += (
                    z[(b, m)] >= pulp.lpSum([x[(r, b)] for r in active_routes]) - 1,
                    f"ConflictMin_z_{b}_{m}"
                )

    # Minimize sum of z_{b,m} across all bays/minutes
    model += pulp.lpSum(z.values()), "TotalConflictMinutes"

    solver = pulp.PULP_CBC_CMD(msg=True)
    result = model.solve(solver)

    print(f"  Solve status: {pulp.LpStatus[result]}")
    if pulp.LpStatus[result] == 'Optimal':
        print(f"  Total conflict minutes (Bay perspective): {pulp.value(model.objective)}")
    else:
        print("  Optimization was not successful.")

    assignment = {}
    if pulp.LpStatus[result] == 'Optimal':
        for r in route_ids:
            for b in bay_labels:
                if pulp.value(x[(r, b)]) > 0.5:
                    assignment[r] = b
                    break
        return assignment, pulp.value(model.objective)
    else:
        return {r: "Unassigned" for r in route_ids}, None

def evaluate_conflicts_per_route(route_to_minutes, assignments):
    """
    From the ROUTE perspective:
      For each route r, how many minutes is it in conflict?
      If route r is in bay B at minute m, and at least 1 other route is also in B at minute m,
      route r is in conflict for that minute.

    Returns {route: conflict_minutes}.
    """
    # Build a bay->minute->list-of-routes mapping
    bay_to_minute_routes = defaultdict(lambda: defaultdict(list))
    for r, bay in assignments.items():
        for m in route_to_minutes[r]:
            bay_to_minute_routes[bay][m].append(r)

    # For each route, count how many of its active minutes are in conflict
    route_conflicts = defaultdict(int)
    for r, bay in assignments.items():
        for m in route_to_minutes[r]:
            overlap = bay_to_minute_routes[bay][m]
            if len(overlap) > 1:
                route_conflicts[r] += 1
    return dict(route_conflicts)

def rebuild_bay_schedules(route_to_minutes, assignments, bay_labels):
    """
    Return minute-by-minute schedules (DataFrame per bay) indicating
    routes present and the conflict count in each minute.
    """
    if route_to_minutes:
        max_minute = max(m for mins in route_to_minutes.values() for m in mins)
    else:
        max_minute = 0

    revised_bay_schedules = {}
    for b in bay_labels:
        routes_in_bay = [r for r, assigned_bay in assignments.items() if assigned_bay == b]

        # Pre-build a dict minute -> which routes are present
        minute_to_routes = defaultdict(list)
        for r in routes_in_bay:
            for m in route_to_minutes[r]:
                minute_to_routes[m].append(r)

        records = []
        for minute in range(0, max_minute + 1):
            present_routes = minute_to_routes[minute]
            conflict_count = max(0, len(present_routes) - 1)
            routes_str = ", ".join(sorted(present_routes)) if present_routes else ""
            time_str = f"{(minute // 60):02d}:{(minute % 60):02d}"

            records.append({
                "minute": minute,
                "time_str": time_str,
                "conflict_count": conflict_count,
                "routes_present_str": routes_str
            })

        bay_df = pd.DataFrame(records)
        revised_bay_schedules[b] = bay_df

    return revised_bay_schedules

###############################################################################
# NEW: EXPORT COMPARISON RESULTS
###############################################################################

def export_comparison_results(
    route_to_minutes,
    default_assignments,
    default_route_conflicts,
    optimized_assignments,
    optimized_route_conflicts,
    bay_labels,
    output_folder,
    output_filename
):
    """
    Exports an Excel file with:
      1) A comparison sheet: route-by-route default vs. optimized assignment & conflict minutes.
      2) A set of sheets for the default minute-by-minute bay schedules.
      3) A set of sheets for the optimized minute-by-minute bay schedules.
    """

    # Ensure output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created output folder: {output_folder}")

    # Prepare the route-level comparison DataFrame
    route_ids = sorted(route_to_minutes.keys())
    comparison_data = []
    for r in route_ids:
        comparison_data.append({
            "route": r,
            "default_bay": default_assignments[r],
            "default_conflict_minutes": default_route_conflicts[r],
            "optimized_bay": optimized_assignments[r],
            "optimized_conflict_minutes": optimized_route_conflicts[r]
        })
    df_comparison = pd.DataFrame(comparison_data)

    # Rebuild schedules for default
    default_schedules = rebuild_bay_schedules(route_to_minutes, default_assignments, bay_labels)
    # Rebuild schedules for optimized
    optimized_schedules = rebuild_bay_schedules(route_to_minutes, optimized_assignments, bay_labels)

    # Write everything to a single Excel
    output_path = os.path.join(output_folder, output_filename)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 1) Comparison sheet
        df_comparison.to_excel(writer, sheet_name="Assignment_Comparison", index=False)

        # 2) Default schedules (one sheet per bay)
        for b, df_bay in default_schedules.items():
            sheet_name = f"Default_{b}"
            if len(sheet_name) > 31:
                sheet_name = sheet_name[:31]
            df_bay.to_excel(writer, sheet_name=sheet_name, index=False)

        # 3) Optimized schedules (one sheet per bay)
        for b, df_bay in optimized_schedules.items():
            sheet_name = f"Optimized_{b}"
            if len(sheet_name) > 31:
                sheet_name = sheet_name[:31]
            df_bay.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\nComparison results exported to: {output_path}\n")


###############################################################################
# MAIN FUNCTION
###############################################################################

def main():
    # 1. Load GTFS
    gtfs_data = load_gtfs_data(CONFIG["gtfs_folder"])

    # 2. Process data -> route_to_minutes
    route_to_minutes = process_gtfs_data(
        gtfs_data,
        CONFIG["whitelisted_service_id"],
        CONFIG["stops_of_interest"]
    )

    # 3. Build bay labels
    bays = []
    for stop_id in CONFIG["stops_of_interest"]:
        num_bays = CONFIG["num_bays_per_stop"].get(stop_id, 1)
        for i in range(num_bays):
            bay_label = f"{stop_id}_Bay{i+1}"
            bays.append(bay_label)

    print(f"\nTotal bays to assign routes to: {len(bays)}")
    print(f"Bays: {bays}")

    # -------------------------------------------------------------------------
    # (A) DEFAULT ASSIGNMENT
    # -------------------------------------------------------------------------
    default_assignments = build_default_assignments(route_to_minutes, bays)
    default_route_conflicts = evaluate_conflicts_per_route(route_to_minutes, default_assignments)
    default_total_route_conflict = sum(default_route_conflicts.values())

    print("\nDEFAULT ASSIGNMENT - ROUTE CONFLICTS:")
    for r in sorted(default_route_conflicts.keys()):
        print(f"  Route {r} => {default_route_conflicts[r]} conflict minutes")
    print(f"Total route conflict minutes (Default) = {default_total_route_conflict}")

    # -------------------------------------------------------------------------
    # (B) OPTIMIZED ASSIGNMENT (ILP)
    # -------------------------------------------------------------------------
    optimized_assignments, total_conflicts_bay_perspective = build_and_solve_ilp(
        route_to_minutes,
        bays,
        CONFIG["stops_of_interest"]
    )
    if total_conflicts_bay_perspective is None:
        print("ILP was infeasible. Consider adjusting # of bays.")
        return

    # Evaluate from the route perspective
    optimized_route_conflicts = evaluate_conflicts_per_route(route_to_minutes, optimized_assignments)
    optimized_total_route_conflict = sum(optimized_route_conflicts.values())

    print("\nOPTIMIZED ASSIGNMENT - ROUTE CONFLICTS:")
    for r in sorted(optimized_route_conflicts.keys()):
        print(f"  Route {r} => {optimized_route_conflicts[r]} conflict minutes")
    print(f"Total route conflict minutes (Optimized) = {optimized_total_route_conflict}")

    # -------------------------------------------------------------------------
    # (C) EXPORT COMPARISON TO EXCEL
    # -------------------------------------------------------------------------
    export_comparison_results(
        route_to_minutes,
        default_assignments,
        default_route_conflicts,
        optimized_assignments,
        optimized_route_conflicts,
        bays,
        CONFIG["output_folder"],
        CONFIG["comparison_output_filename"]
    )

    # -------------------------------------------------------------------------
    # Print final summary
    # -------------------------------------------------------------------------
    print("\nFINAL (OPTIMIZED) ROUTE ASSIGNMENTS:")
    for r, b in sorted(optimized_assignments.items()):
        print(f"  Route {r} => {b}")

    print("\nTotal conflict minutes (Bay perspective):")
    print(f"  {total_conflicts_bay_perspective} (across all bays)")

    print("\nTotal conflict minutes (Route perspective):")
    print(f"  {optimized_total_route_conflict} (sum of route conflict-minutes)")

if __name__ == "__main__":
    main()
