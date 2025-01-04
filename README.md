# Transit Planning With Python

Transit Planning Tools for Medium-Sized Agencies

Welcome to the transit_planning_with_python repository! This collection of off-the-shelf Python scripts is designed to assist transit planners at medium-sized agencies. Each tool focuses on accomplishing a common task or solving a common challenge in transit analysis and planning. The most common input is static ***General Transit Feed Specification (GTFS)*** data, but shapefile roadway centerlines, roadway polygons, bus route shapes, Census blocks, and others are also commonly used.

## 🚀 Features

Our Python scripts automate complex transit analysis tasks, making data-driven planning easier and faster. Key features include:

Data Preparation and Visualization

- ***Ridership Maps:*** Automate the processing of ridership, stop, and area data for use in heatmaps.
- ***Choropleth Data:*** Generate ready-to-map data for ridership trends and demographic overlays.

Operational Analysis

- ***Bus Bay Conflict Checker:*** Identify scheduling conflicts at bus bays where multiple buses overlap in time and space.
- ***Headway and Schedule Analysis:*** Automatically calculate headways, span, and trip frequencies for any schedule.

Fieldwork Tools

- ***On-Site Checklists:*** Create field checklists for monitoring on-time performance and ridership at stops or while riding routes.

Network Analysis

- ***Route Identification:*** Quickly find transit routes operating near a defined point.
- ***Isochrone and Accessibility*** Mapping: Build transit and roadway networks to analyze travel times and service coverage.

GTFS Validation and Export

- ***GTFS Location and Name Validation:*** Cross-check GTFS stop and route data against standard shapefiles like roadway_centerlines.shp and bus_routes.shp.
- ***Schedule Export:*** Export transit schedules directly to Excel (.xlsx) format for sharing and reporting.

Demographic Analysis

- ***Census Integration:*** Process and combine Census tables with shapefiles for demographic insights.
- ***Service Population Analysis:*** Calculate residential and employee populations served by individual transit routes and systems.

## 📂 Repository Structure

The repository is organized for ease of use, with:

- ***Standalone scripts:*** Each tool is fully documented with comments explaining field name assumptions, file formats, and usage instructions.
- ***Standard data formats:*** Most scripts are designed to work with commonly used data types like GTFS files and shapefiles. Specific requirements are outlined in the script comments.

## 🛠️ Requirements

- Python 3.7+
- Common libraries like pandas, geopandas, rapidfuzz, networkx, and others listed in requirements.txt.

## 🧑‍💻 How to Use

The **transit_planning_with_python** tools are designed to work on most systems with Python installed. However, if you're using a work computer configured with ArcPro, there are additional steps required due to restrictions on installing libraries like `geopandas`. Alternately, if you are using a home computer without ArcPro, you will not have access to the arcpy library.

---

### Option A: Setting Up Python on a Home Computer (no arcpy)

1. **Download and Install Python:**
   - Visit the [official Python website](https://www.python.org/downloads/) and download Python 3.9+.
   - During installation, ensure you check the option to **"Add Python to PATH"**.

2. **Install Jupyter Notebook and Required Libraries:**
   - Run the following command in your Command Prompt:
     ```bash
     pip install jupyterlab pandas geopandas rapidfuzz matplotlib networkx
     ```

3. **Launch Jupyter Notebook:**
   - After installation, you can find "Jupyter Notebook" like any other program on your computer. Clicking it will open your default browser with the notebook interface.
   - Alternately, you can run this command in the Command Prompt to launch Jupyter Notebook:
     ```bash
     jupyter notebook
     ```

4. **Download Desired Scripts:**
   - Navigate to scripts that are useful to you and your agency. Then copy and paste their contents into your active Jupyter Notebook or download them as .py files. You do not need a        GitHub account to do this.
   - Alternately, you can clone or download the whole repository from GitHub.
 
5. **Run the Script:**
   - Follow the instructions provided in the comments of the script(s).
   - At a minimum, you will need to update the folder and file paths to point to your data and desired location for any output.
   - Run the script, follow any printed instructions to resolve issues, and check the output for accuracy.

---

### Option B: Using Python on a Work Computer with ArcPro (no geopandas)

1. **Open a Notebook:**
   - If ArcPro is installed on your computer, then Python is as well.
   - You can launch ArcPro and then open, create, or save a Notebook file (.ipynb) within that program.
   - You can also find "Jupyter Notebook" like any other program on your computer. Clicking it will open your default browser with the notebook interface.
   - Alternately, you can run this command in the Command Prompt to launch Jupyter Notebook:
     ```bash
     jupyter notebook
     ```

2. **Download Desired Scripts:**
   - Navigate to scripts that are useful to you and your agency. Then copy and paste their contents into your active Notebook or download them as .py files. You do not need a               GitHub account to do this.
   - Alternately, you can clone or download the whole repository from GitHub.
 
3. **Run the Script:**
   - Follow the instructions provided in the comments of the script(s).
   - At a minimum, you will need to update the folder and file paths to point to your data and desired location for any output.
   - Run the script, follow any printed instructions to resolve issues, and check the output for accuracy.

---

### What is Jupyter Notebook?

Jupyter Notebook is a powerful tool for running Python scripts in an interactive environment that opens in your web browser. It allows you to write, test, and visualize Python code in a more user-friendly way.

---

### Tips for Success

- **File Paths:** Ensure all file paths are updated in the scripts to match your local system.
- **Permissions:** If IT restrictions block library installations on your work computer, setting up Python on a home computer is recommended.

## 🤝 Contributing

We welcome your contributions! Feel free to open an issue or submit a pull request.

## 📄 License

We hope these tools speed up and simplify your transit planning workflows! If you encounter issues or have questions, feel free to open an issue or contact us. 🚍
