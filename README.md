# transit_planning_with_python

Transit Planning Tools for Medium-Sized Agencies

Welcome to the transit_planning_with_python repository! This collection of off-the-shelf Python scripts is designed to assist transit planners at medium-sized agencies. Each tool focuses on accomplishing a common task or solving a common challenge in transit analysis and planning. The most common input is static General Transit Feed Specification (GTFS) data, but shapefile roadway centerlines, roadway polygons, bus route shapes, Census blocks, and others are also commonly used.

🚀 Features

📂 Repository Structure

The repository is organized for ease of use, with:

- Standalone scripts: Each tool is fully documented with comments explaining field name assumptions, file formats, and usage instructions.
- Standard data formats: Most scripts are designed to work with commonly used data types like GTFS files and shapefiles. Specific requirements are outlined in the script comments.

transit_planning_with_python/
├── README.md
├── LICENSE
├── .github/
│   └── workflows/
│       └── pylint.yml
├── arcpy_tools/
│   └── bus_stops_ridership_joiner.py
├── gtfs_data_analysis/
│   ├── gtfs_bus_bay_conflict_checker.py
│   ├── gtfs_headway_span_trips_calculator.py
│   ├── gtfs_trips_hourly_reporter.py
│   └── manual_gtfs_nearby_routes.py
├── gtfs_field_resources/
│   ├── gtfs_bus_arrivals_checklist_cluster_validation.py
│   ├── gtfs_bus_arrivals_checklist_printable.py
│   ├── gtfs_bus_arrivals_checklist_processing.py
│   ├── gtfs_schedule_exporter.py
│   ├── gtfs_stop_boardings_by_route_printable.py
│   └── gtfs_timepoints_by_route_printable.py
├── gtfs_validation/
│   ├── gtfs_recovery_dwell_time_conflict_check.py
│   ├── gtfs_stop_capitalization_suffix_checker.py
│   ├── gtfs_stop_road_shp_typo_finder.py
│   ├── gtfs_stop_roadway_shp_intersection_check.py
│   └── gtfs_to_system_shp_checker.py
├── network_analysis/
│   └── park_and_ride_accessibility.py
└── service_population_tools/
    ├── census_processing_csv_shp.py
    ├── gtfs_demogs_served_calcs_network.py
    └── gtfs_demogs_served_calcs_routes.py

🛠️ Requirements
- Python 3.7+
- Common libraries like pandas, geopandas, partridge, gtfs-kit, and others listed in requirements.txt.

🧑‍💻 How to Use

🤝 Contributing
We welcome your contributions! Feel free to open an issue or submit a pull request.

📄 License

We hope these tools simplify your transit planning workflows! If you encounter issues or have questions, feel free to open an issue or contact us. 🚍
