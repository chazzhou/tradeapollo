# Energy Consumption Production Visualizations Readme

Visualizations that we didn't include in map, but are working

## Features

- **Energy Source Composition**: Visualizes the composition of various energy sources for a specified symbol on a selected date, using a pie chart.
- **Production vs. Consumption**: Compares energy production against consumption throughout a day for a chosen symbol and date, presented in a stacked area chart.
- **Market Dynamics**: Displays the hourly percentage change in imbalance price for a specified symbol, reflecting the fluctuations in energy prices.

## Installation

Ensure Python is installed on your system. Install the required libraries with:

```bash
pip install streamlit pandas matplotlib
```

## Usage

- Prepare your CSV datasets (capacity.csv, production.csv, consumption.csv, balance.csv) and place them in a known directory. Adjust the file paths in the script accordingly.
- Run the Streamlit application by executing:

streamlit run <script_name>.py

Replace <script_name>.py with the path to your script.

1.Use the Streamlit web interface to select a symbol (area) and date, then click "Show Plots" to generate the visualizations.

## Key Functions

plot_energy_sources_pie_with_other(): Generates a pie chart of energy sources for a given symbol and date, grouping minor sources under "Other".
plot_production_vs_consumption_revised(): Creates a stacked area chart to compare energy production and consumption for a selected symbol and date.
plot_percentage_change_for_symbol(): Plots the hourly percentage change in the imbalance price for a specified symbol.
