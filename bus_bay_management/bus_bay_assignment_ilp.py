#!/usr/bin/env python
# coding: utf-8
"""
This script optimizes bus bay assignments by analyzing GTFS data.
- Conflict analysis is performed at the block level (each block represents a physical bus).
- Each route is assigned to exactly one bay.
- Interlined routes (same block serving multiple routes at different times) can be assigned to different bays.
"""

import os
import re
from collections import defaultdict

import pandas as pd
import pulp  # Required for ILP
from pulp import LpVariable, LpProblem, LpMinimize, lpSum, LpBinary, LpInteger, LpStatus, value

###############################################################################
# CONFIGURATION
###############################################################################

CONFIG = {
    "gtfs_folder": r"C:\Your\Path\To\GTFS_data_folder",
    "whitelisted_service_id": '1',
    "stops_of_interest": ['1001', '1002'],
    "num_bays_per_stop": {
        "1001": 1,
        "1002": 1
    },
    "output_folder": r"C:\Your\Folder\Path\for\Output",
    "comparison_output_filename": "BayAssignment_Comparison.xlsx",
    "extended_offservice_threshold": 60  # in minutes
}

LAYOVER_THRESHOLD = 15  # in minutes

###############################################################################
# SNIPPET-STYLE HELPER FUNCTIONS
###############################################################################

def time_to_seconds(time_str):
    """
    Convert HH:MM:SS format to total seconds, even beyond 24:00:00.
    """
    parts = time_str.split(":")
    hours, minutes, seconds = map(int, parts)
    return hours * 3600 + minutes * 60 + seconds

def get_trip_ranges_and_ends(block_segments):
    """
    Return a list of tuples with trip-related info:
    (trip_id, trip_start, trip_end, route_short_name, direction_id, start_stop, end_stop).
    """
    trips_info = []
    unique_trips = block_segments['trip_id'].unique()
    for trip_id in unique_trips:
        tsub = block_segments[block_segments['trip_id'] == trip_id].sort_values('arrival_seconds')
        trip_start = tsub['arrival_seconds'].min()
        trip_end = tsub['departure_seconds'].max()
        route_short_name = tsub['route_short_name'].iloc[0]
        direction_id = tsub['direction_id'].iloc[0]
        start_stop = tsub.iloc[0]['stop_id']
        end_stop = tsub.iloc[-1]['stop_id']
        trips_info.append((
            trip_id,
            trip_start,
            trip_end,
            route_short_name,
            direction_id,
            start_stop,
            end_stop
        ))
    trips_info.sort(key=lambda x: x[1])  # sort by trip_start
    return trips_info

def get_minute_status_location_complex(
    minute: int,
    block_segments: pd.DataFrame,
    trips_info: list,
    layover_threshold: int = LAYOVER_THRESHOLD,
    extended_offservice_threshold: int = CONFIG["extended_offservice_threshold"]
):
    """
    Determine the bus's status and location at a given minute for the given block.
    Returns (status, location, route_short_name, direction_id, stop_id).
    """
    current_sec = minute * 60

    if not trips_info:
        # No trips in this block => always inactive
        return ("inactive", "inactive", "", "", "")

    # Identify earliest start and latest end among all trips
    earliest_start = min(trp[1] for trp in trips_info)
    latest_end = max(trp[2] for trp in trips_info)

    # If we’re before the first trip or after the last trip
    if current_sec < earliest_start or current_sec > latest_end:
        return ("inactive", "inactive", "", "", "")

    # Check if we are in the window of a specific trip
    active_trip_idx = None
    for idx, (tid, tstart, tend, rname, dirid, stp_st, stp_end) in enumerate(trips_info):
        if tstart <= current_sec <= tend:
            active_trip_idx = idx
            break

    if active_trip_idx is not None:
        (tid, tstart, tend, rname, dirid, start_stp, end_stp) = trips_info[active_trip_idx]
        tsub = block_segments[block_segments['trip_id'] == tid].sort_values('arrival_seconds')

        # Step through each stop in this trip
        for _, row in tsub.iterrows():
            arr_sec = row['arrival_seconds']
            dep_sec = row['departure_seconds']
            next_stp = row['next_stop_id']
            next_arr = row['next_arrival_seconds']

            # Dwelling at the current stop
            if arr_sec <= current_sec <= dep_sec:
                return ("dwelling at stop", row['stop_id'], rname, dirid, row['stop_id'])

            # traveling vs. layover
            if pd.notnull(next_arr):
                narr_sec = int(next_arr)
                if dep_sec < current_sec < narr_sec:
                    # Could be traveling or a short layover
                    if next_stp == row['stop_id']:
                        gap = narr_sec - dep_sec
                        if gap > layover_threshold * 60:
                            return ("laying over", row['stop_id'], rname, dirid, row['stop_id'])
                        return ("running route", "traveling between stops", rname, dirid, "")
                    return ("running route", "traveling between stops", rname, dirid, "")

        # If we reach here, we might be beyond the last stop's departure_seconds
        return ("laying over", end_stp, rname, dirid, end_stp)

    # We’re between trips within this block. Determine short layover vs off-service
    prev_trip = None
    next_trip = None
    for idx, (tid, tstart, tend, rname, dirid, stp_st, stp_end) in enumerate(trips_info):
        if tend < current_sec:
            prev_trip = (tid, tstart, tend, rname, dirid, stp_st, stp_end)
        if current_sec < tstart and next_trip is None:
            next_trip = (tid, tstart, tend, rname, dirid, stp_st, stp_end)
            break

    if not prev_trip:
        # We haven't reached the first trip's start yet
        return ("inactive", "inactive", "", "", "")

    (ptid, ptstart, ptend, prname, pdirid, pstart_stp, pend_stp) = prev_trip
    gap_to_next_trip = None
    if next_trip:
        (ntid, ntstart, ntend, nrname, ndirid, nstart_stp, nend_stp) = next_trip
        gap_to_next_trip = ntstart - ptend

    if gap_to_next_trip is not None:
        if gap_to_next_trip <= layover_threshold * 60:
            return ("laying over", pend_stp, prname, pdirid, pend_stp)
        return ("off-service", "inactive", "", "", "")

    # No next trip => after the last trip but inside earliest..latest => off-service
    return ("off-service", "inactive", "", "", "")

###############################################################################
# LOADING & PROCESSING GTFS
###############################################################################

def load_gtfs_data(gtfs_folder):
    """
    Load essential GTFS CSVs into DataFrames.
    """
    required_files = ["trips.txt", "stop_times.txt", "routes.txt", "stops.txt", "calendar.txt"]
    data = {}
    for file in required_files:
        path = os.path.join(gtfs_folder, file)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required GTFS file not found: {path}")
        print(f"Loading {file}...")
        data[file.split('.', maxsplit=1)[0]] = pd.read_csv(path, dtype=str)
    return data

def build_route_occupancy(block_segments, stops_of_interest):
    """
    Build a dictionary mapping each route to the minutes it has buses at the stops of interest.
    
    Parameters
    ----------
    block_segments : pd.DataFrame
        DataFrame containing stop_times for all blocks.
    stops_of_interest : list of str
        List of stop IDs to consider for occupancy.

    Returns
    -------
    dict
        {route_short_name: {minute: number_of_buses_present}}
    """
    route_occupancy = defaultdict(lambda: defaultdict(int))

    # Identify all unique blocks
    blocks = block_segments['block_id'].unique()

    for blk in blocks:
        block_data = block_segments[block_segments['block_id'] == blk].copy()
        if block_data.empty:
            continue

        trips_info = get_trip_ranges_and_ends(block_data)
        if not trips_info:
            continue

        min_sec = min(t[1] for t in trips_info)
        max_sec = max(t[2] for t in trips_info)
        start_min = min_sec // 60
        end_min = max_sec // 60

        for minute in range(start_min, end_min + 1):
            status, location, rname, dirid, stop_id = get_minute_status_location_complex(
                minute, block_data, trips_info
            )
            if status in ["dwelling at stop", "laying over"] and location in stops_of_interest:
                if rname:  # Ensure route_short_name isn't empty
                    route_occupancy[rname][minute] += 1

    # Convert inner defaultdicts to normal dicts
    route_occupancy = {route: dict(mins) for route, mins in route_occupancy.items()}
    return route_occupancy

###############################################################################
# CONFLICT & ASSIGNMENT LOGIC
###############################################################################

def build_default_assignments(route_to_minutes, bay_labels):
    """
    A trivial "default" assignment: round-robin each route among the available bays.

    Parameters
    ----------
    route_to_minutes : dict
        {route_short_name: {minute: occupant_count}}
    bay_labels : list of str
        The bays we have available.

    Returns
    -------
    dict
        {route_short_name: bay_label}
    """
    default_assignment = {}
    route_ids = sorted(route_to_minutes.keys())
    idx = 0
    for route_id in route_ids:
        default_assignment[route_id] = bay_labels[idx % len(bay_labels)]
        idx += 1
    return default_assignment

def build_and_solve_ilp_one_bay_per_route(route_occupancy, bay_labels, stop_ids):
    """
    ILP: One bay per route, conflict counted using route-level occupancy
         (derived from block-level presence).

    Parameters
    ----------
    route_occupancy : dict
      {route_short_name: {minute: occupant_count}}, occupant_count = # of buses from route at minute M.
    bay_labels : list of str
      All possible bays to assign each route to.
    stop_ids : list of str
      For logging only.

    Returns
    -------
    (route_assignments, total_conflict)
      route_assignments: {route: bay_label}
      total_conflict: float
    """
    print(f"\n=== ILP: One Bay per Route, conflict from block-level presence ===")
    print(f"Stops of interest = {stop_ids}")

    # 1) Gather all routes and all relevant minutes
    all_routes = sorted(route_occupancy.keys())
    all_minutes = sorted({m for rocc in route_occupancy.values() for m in rocc.keys()})

    # 2) Create the model
    model = LpProblem("OneBayPerRoute_BlocksConflict", LpMinimize)

    # Binary var x_{r,bay} = 1 if route r is assigned to bay
    x = {}
    for r in all_routes:
        for bay in bay_labels:
            safe_r = re.sub(r'[^A-Za-z0-9]', '_', r)
            safe_bay = re.sub(r'[^A-Za-z0-9]', '_', bay)
            var_name = f"x_{safe_r}_{safe_bay}"
            x[(r, bay)] = LpVariable(var_name, cat=LpBinary)

    # Each route must be assigned to exactly one bay
    for r in all_routes:
        model += (
            lpSum(x[(r, bay)] for bay in bay_labels) == 1,
            f"OneBayForRoute_{r}"
        )

    # Conflict var z_{bay,minute} >= occupantCount_{bay,minute} - 1
    # occupantCount_{bay,minute} = sum of (occupancy_r(m) * x_{r,bay})
    z = {}
    for bay in bay_labels:
        safe_bay = re.sub(r'[^A-Za-z0-9]', '_', bay)
        for minute in all_minutes:
            var_name = f"z_{safe_bay}_{minute}"
            z[(bay, minute)] = LpVariable(var_name, lowBound=0, cat=LpInteger)

            # occupantCount = sum of (occupancy_r(m) * x_{r,bay})
            occupant_expr = lpSum(
                route_occupancy[r].get(minute, 0) * x[(r, bay)]
                for r in all_routes
            )

            # Constraint: z_{bay,minute} >= occupantCount - 1
            model += (
                z[(bay, minute)] >= occupant_expr - 1,
                f"ConflictMin_{bay}_{minute}"
            )

    # Objective: minimize the sum of all z_{bay,minute}
    model += lpSum(z.values()), "TotalConflicts"

    # 3) Solve
    solver = pulp.PULP_CBC_CMD(msg=True)
    result = model.solve(solver)

    print(f" Solve status: {LpStatus[result]}")
    if LpStatus[result] != "Optimal":
        print(" Optimization was not successful.")
        return {}, None

    total_conflicts = value(model.objective)
    print(f" Total conflict minutes = {total_conflicts}")

    # 4) Extract solution
    route_assignments = {}
    for r in all_routes:
        chosen_bay = None
        for bay in bay_labels:
            var_value = value(x[(r, bay)])
            if var_value > 0.5:
                chosen_bay = bay
                break
        route_assignments[r] = chosen_bay

    return route_assignments, total_conflicts

def evaluate_conflicts_per_route(route_to_minutes, assignments):
    """
    From the ROUTE perspective, count conflicts for each route.

    If route R is in bay B at minute M, and at least one other route is also
    in B at minute M, then route R is in conflict for that minute.

    Parameters
    ----------
    route_to_minutes : dict
        {route_short_name: {minute: occupant_count}}
    assignments : dict
        {route_short_name: bay_label}

    Returns
    -------
    dict
        {route_short_name: conflict_minutes}
    """
    # Build bay->minute->list-of-routes
    bay_to_minute_routes = defaultdict(lambda: defaultdict(list))
    for route_id, bay_assigned in assignments.items():
        for minute, count in route_to_minutes[route_id].items():
            # Each count represents 'count' buses from this route at this minute
            for _ in range(count):
                bay_to_minute_routes[bay_assigned][minute].append(route_id)

    # For each route, count how many of its active minutes are in conflict
    route_conflicts = defaultdict(int)
    for route_id, bay_assigned in assignments.items():
        for minute, count in route_to_minutes[route_id].items():
            # Total buses from all routes at this bay and minute
            total_buses = len(bay_to_minute_routes[bay_assigned][minute])
            if total_buses > 1:
                route_conflicts[route_id] += count  # Each bus counts separately

    return dict(route_conflicts)

def rebuild_bay_schedules(route_to_minutes, assignments, bay_labels):
    """
    Construct minute-by-minute schedules for each bay: which routes are present?
    Useful for debugging or exporting to Excel. Return {bay_label: DataFrame}.
    """
    if not route_to_minutes:
        return {}

    max_minute = max(m for mins in route_to_minutes.values() for m in mins.keys())
    bay_schedules = {}

    for bay in bay_labels:
        # Which routes are assigned to this bay
        routes_in_bay = [r for r, a in assignments.items() if a == bay]

        # Minute -> which routes are present
        minute_to_routes = defaultdict(list)
        for route_id in routes_in_bay:
            for minute, count in route_to_minutes[route_id].items():
                for _ in range(count):
                    minute_to_routes[minute].append(route_id)

        records = []
        for minute in range(0, max_minute + 1):
            present_routes = minute_to_routes.get(minute, [])
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
        bay_schedules[bay] = bay_df

    return bay_schedules

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
    Exports an Excel file comparing default and optimized assignments, plus
    minute-by-minute schedules for both.

    Parameters
    ----------
    route_to_minutes : dict
        {route_short_name: {minute: occupant_count}}
    default_assignments : dict
        {route_short_name: bay_label} for the default approach
    default_route_conflicts : dict
        {route_short_name: conflict_minutes}
    optimized_assignments : dict
        {route_short_name: bay_label} for the optimized approach
    optimized_route_conflicts : dict
        {route_short_name: conflict_minutes}
    bay_labels : list of str
        Bays to iterate over
    output_folder : str
        Destination folder for the Excel
    output_filename : str
        Filename for the Excel
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created output folder: {output_folder}")

    # Prepare comparison DataFrame
    route_ids = sorted(route_to_minutes.keys())
    comparison_data = []
    for route_id in route_ids:
        comparison_data.append({
            "route": route_id,
            "default_bay": default_assignments.get(route_id, "Unassigned"),
            "default_conflict_minutes": default_route_conflicts.get(route_id, 0),
            "optimized_bay": optimized_assignments.get(route_id, "Unassigned"),
            "optimized_conflict_minutes": optimized_route_conflicts.get(route_id, 0)
        })
    df_comparison = pd.DataFrame(comparison_data)

    # Rebuild schedules
    default_schedules = rebuild_bay_schedules(route_to_minutes, default_assignments, bay_labels)
    optimized_schedules = rebuild_bay_schedules(route_to_minutes, optimized_assignments, bay_labels)

    output_path = os.path.join(output_folder, output_filename)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 1) Comparison sheet
        df_comparison.to_excel(writer, sheet_name="Assignment_Comparison", index=False)

        # 2) Default schedules (one sheet per bay)
        for bay_label, bay_df in default_schedules.items():
            sheet_name = f"Default_{bay_label}"
            if len(sheet_name) > 31:
                sheet_name = sheet_name[:31]
            bay_df.to_excel(writer, sheet_name=sheet_name, index=False)

        # 3) Optimized schedules (one sheet per bay)
        for bay_label, bay_df in optimized_schedules.items():
            sheet_name = f"Optimized_{bay_label}"
            if len(sheet_name) > 31:
                sheet_name = sheet_name[:31]
            bay_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\nComparison results exported to: {output_path}\n")

###############################################################################
# MAIN FUNCTION
###############################################################################

def main():
    """
    Main entry point: load GTFS, process data, build default & optimized assignments,
    and export comparison results.
    """
    # 1. Load GTFS
    gtfs_data = load_gtfs_data(CONFIG["gtfs_folder"])

    # 2. Process data -> route_to_minutes and block_to_minutes
    #    We'll focus on building route_to_minutes for ILP
    stop_times = gtfs_data['stop_times']
    trips = gtfs_data['trips']
    routes = gtfs_data['routes'][['route_id', 'route_short_name']]

    # Filter trips by service_id
    trips = trips[trips['service_id'] == CONFIG['whitelisted_service_id']]
    print(f"Filtered trips to service_id '{CONFIG['whitelisted_service_id']}': {len(trips)} trips")

    # Merge route info into trips
    trips = trips.merge(routes, on='route_id', how='left')

    # Merge trips info into stop_times
    stop_times = stop_times[stop_times['trip_id'].isin(trips['trip_id'])]
    stop_times = stop_times.merge(
        trips[['trip_id', 'route_short_name', 'block_id', 'direction_id']],
        on='trip_id',
        how='left'
    )

    # Convert arrival/departure times to seconds
    stop_times['arrival_seconds'] = stop_times['arrival_time'].apply(time_to_seconds)
    stop_times['departure_seconds'] = stop_times['departure_time'].apply(time_to_seconds)

    # Sort stop_times
    stop_times.sort_values(['block_id', 'trip_id', 'stop_sequence'], inplace=True)

    # Create next_stop_id and next_arrival_seconds for each trip
    stop_times['next_stop_id'] = stop_times.groupby('trip_id')['stop_id'].shift(-1)
    stop_times['next_arrival_seconds'] = stop_times.groupby('trip_id')['arrival_seconds'].shift(-1)

    # 3. Build route occupancy dictionary
    route_to_minutes = build_route_occupancy(stop_times, CONFIG["stops_of_interest"])
    print("Built route_to_minutes using block-level presence.")

    # 4. Build the list of bays
    bay_labels = []
    for stop_id in CONFIG["stops_of_interest"]:
        num_bays = CONFIG["num_bays_per_stop"].get(stop_id, 1)
        for idx in range(num_bays):
            bay_label = f"{stop_id}_Bay{idx+1}"
            bay_labels.append(bay_label)

    print(f"\nTotal bays to assign routes to: {len(bay_labels)}")
    print(f"Bays: {bay_labels}")

    # -------------------------------------------------------------------------
    # (A) DEFAULT ASSIGNMENT
    # -------------------------------------------------------------------------
    default_assignments = build_default_assignments(route_to_minutes, bay_labels)
    default_route_conflicts = evaluate_conflicts_per_route(route_to_minutes, default_assignments)
    default_total_conflict = sum(default_route_conflicts.values())

    print("\nDEFAULT ASSIGNMENT - ROUTE CONFLICTS:")
    for route_id in sorted(default_route_conflicts.keys()):
        print(f"  Route {route_id} => {default_route_conflicts[route_id]} conflict minutes")
    print(f"Total route conflict minutes (Default) = {default_total_conflict}")

    # -------------------------------------------------------------------------
    # (B) OPTIMIZED ASSIGNMENT (ILP)
    # -------------------------------------------------------------------------
    optimized_assignments, total_conflicts_bay_perspective = build_and_solve_ilp_one_bay_per_route(
        route_to_minutes,
        bay_labels,
        CONFIG["stops_of_interest"]
    )
    if total_conflicts_bay_perspective is None:
        print("ILP was infeasible or not optimal. Consider adjusting the number of bays.")
        return

    # Evaluate from the route perspective
    optimized_route_conflicts = evaluate_conflicts_per_route(route_to_minutes, optimized_assignments)
    optimized_total_route_conflict = sum(optimized_route_conflicts.values())

    print("\nOPTIMIZED ASSIGNMENT - ROUTE CONFLICTS:")
    for route_id in sorted(optimized_route_conflicts.keys()):
        print(f"  Route {route_id} => {optimized_route_conflicts[route_id]} conflict minutes")
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
        bay_labels,
        CONFIG["output_folder"],
        CONFIG["comparison_output_filename"]
    )

    # -------------------------------------------------------------------------
    # (D) PRINT FINAL SUMMARY
    # -------------------------------------------------------------------------
    print("\nFINAL (OPTIMIZED) ROUTE ASSIGNMENTS:")
    for route_id, bay_label in sorted(optimized_assignments.items()):
        print(f"  Route {route_id} => {bay_label}")

    print("\nTotal conflict minutes (Bay perspective):")
    print(f"  {total_conflicts_bay_perspective} (across all bays)")

    print("\nTotal conflict minutes (Route perspective):")
    print(f"  {optimized_total_route_conflict} (sum of route conflict-minutes)")

    # -------------------------------------------------------------------------
    # (E) OPTIONAL: Build minute-by-minute bay schedules (for debugging/Excel)
    # -------------------------------------------------------------------------
    final_bay_schedules = rebuild_bay_schedules(route_to_minutes, optimized_assignments, bay_labels)
    # To export these schedules, you can modify the export_comparison_results function or add another export step.

    print("\nDone.")

if __name__ == "__main__":
    main()
