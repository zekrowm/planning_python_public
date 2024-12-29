# transit_planning_with_python

Transit Planning Tools for Medium-Sized Agencies

Welcome to the transit_planning_with_python repository! This collection of off-the-shelf Python scripts is designed to assist transit planners at medium-sized agencies. Each tool focuses on accomplishing a common task or solving a common challenge in transit analysis and planning. The most common input is static General Transit Feed Specification (GTFS) data, but shapefile roadway centerlines, roadway polygons, bus route shapes, Census blocks, and others are also commonly used.

🚀 Features

Our Python scripts automate complex transit analysis tasks, making data-driven planning easier and faster. Key features include:

Data Preparation and Visualization

- Ridership Maps: Automate the processing of ridership, stop, and area data for use in heatmaps.
- Choropleth Data: Generate ready-to-map data for ridership trends and demographic overlays.

Operational Analysis

- Bus Bay Conflict Checker: Identify scheduling conflicts at bus bays where multiple buses overlap in time and space.
- Headway and Schedule Analysis: Automatically calculate headways, span, and trip frequencies for any schedule.

Fieldwork Tools

- On-Site Checklists: Create field checklists for monitoring on-time performance and ridership at stops or while riding routes.

Network Analysis

- Route Identification: Quickly find transit routes operating near a defined point.
- Isochrone and Accessibility Mapping: Build transit and roadway networks to analyze travel times and service coverage.

GTFS Validation and Export

- GTFS Location and Name Validation: Cross-check GTFS stop and route data against standard shapefiles like roadway_centerlines.shp and bus_routes.shp.
- Schedule Export: Export transit schedules directly to Excel (.xlsx) format for sharing and reporting.

Demographic Analysis

- Census Integration: Process and combine Census tables with shapefiles for demographic insights.
- Service Population Analysis: Calculate residential and employee populations served by individual transit routes and systems.

📂 Repository Structure

The repository is organized for ease of use, with:

- Standalone scripts: Each tool is fully documented with comments explaining field name assumptions, file formats, and usage instructions.
- Standard data formats: Most scripts are designed to work with commonly used data types like GTFS files and shapefiles. Specific requirements are outlined in the script comments.

🛠️ Requirements
- Python 3.7+
- Common libraries like pandas, geopandas, partridge, gtfs-kit, and others listed in requirements.txt.

🧑‍💻 How to Use

🤝 Contributing

We welcome your contributions! Feel free to open an issue or submit a pull request.

📄 License

We hope these tools simplify your transit planning workflows! If you encounter issues or have questions, feel free to open an issue or contact us. 🚍
