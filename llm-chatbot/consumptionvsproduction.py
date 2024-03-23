import streamlit as st
import pandas as pd
from collections import defaultdict
import json 
import matplotlib.pyplot as plt

def plot_energy_sources_pie_with_other(df, symbol, date, threshold=1.0):
    # Filter data for the given symbol and date
    mask = (df['symbol'] == symbol) & (df['deliveryStart'].str.startswith(date))
    filtered_df = df.loc[mask]
    
    # Exclude non-numeric columns and zero-valued parts before summing
    numeric_cols = filtered_df.select_dtypes(include='number').columns.drop(['total'])
    energy_sources = filtered_df[numeric_cols].sum()
    energy_sources = energy_sources[energy_sources > 0]  # Exclude zero-valued parts

    # Aggregate small categories into 'Other'
    total = energy_sources.sum()
    small_categories = energy_sources[energy_sources / total * 100 < threshold]
    energy_sources = energy_sources[energy_sources / total * 100 >= threshold]
    if not small_categories.empty:
        energy_sources['Other'] = small_categories.sum()
    
    # Plot
    fig, ax = plt.subplots()
    wedges, texts, autotexts = ax.pie(energy_sources, labels=energy_sources.index, autopct='%1.1f%%', startangle=140)
    plt.title(f'Energy Sources for {symbol} on {date}')
    
    # I
    # Improve display
    ax.axis('equal')  # Equal aspect ratio ensures the pie chart is circular.
    plt.tight_layout()

    # Adjust text to prevent overlap
    for text in texts:
        text.set_size(8)
    for autotext in autotexts:
        autotext.set_size(8)
    
    return fig

def extract_consumption_volume(row, symbol):
    """Extract consumption volume from the 'entryPerArea' column for the given symbol."""
    entry = json.loads(row['entryPerArea'].replace("'", "\""))
    if symbol in entry:
        return entry[symbol]['volume']
    else:
        return 0

def prepare_data_fixed(production_data, consumption_data, date, symbol):
    """
    Prepare data with fixed handling for plotting, ensuring compatibility with matplotlib.
    """
    # Filter and prepare production data
    prod_data_filtered = production_data[(production_data['deliveryStart'].str.startswith(date)) & 
                                         (production_data['symbol'] == symbol)]
    prod_data_filtered['hour'] = pd.to_datetime(prod_data_filtered['deliveryStart']).dt.hour
    prod_summary = prod_data_filtered.groupby('hour')['total'].sum().reset_index()

    # Prepare consumption data
    consumption_data['hour'] = pd.to_datetime(consumption_data['deliveryStart']).dt.hour
    consumption_data['consumption_volume'] = consumption_data.apply(lambda row: extract_consumption_volume(row, symbol), axis=1)
    cons_summary = consumption_data.groupby('hour')['consumption_volume'].mean().reset_index()

    # Merging datasets
    merged_data = pd.merge(prod_summary, cons_summary, on='hour', how='outer').sort_values(by='hour')
    merged_data.fillna(0, inplace=True)  # Fill missing values with 0

    return merged_data

def plot_production_vs_consumption_revised(production_data, consumption_data, date, symbol):
    """
    Revised plotting function to ensure data compatibility and address previous issues.
    """
    data = prepare_data_fixed(production_data, consumption_data, date, symbol)

    plt.figure(figsize=(12, 6))
    plt.stackplot(data['hour'], data[['total', 'consumption_volume']].T, labels=['Production', 'Consumption'], alpha=0.5)
    plt.title(f'Production vs Consumption on {date} for {symbol}')
    plt.xlabel('Hour of the Day')
    plt.ylabel('Volume')
    plt.xticks(range(0, 24))
    plt.legend(loc='upper left')
    plt.grid(True)
    return plt

def plot_percentage_change_for_symbol(symbol, data):
    """
    Plots the hourly percentage change in imbalancePrice for a given symbol.
    
    Parameters:
    - symbol: The symbol (area) for which to plot the data (e.g., 'DK1').
    """
    # Redefine the extraction function to work for any given symbol
    def extract_symbol_data(entry):
        try:
            corrected_entry = entry.replace("'", "\"").replace("True", "true").replace("False", "false").replace("None", "null")
            parsed_entry = json.loads(corrected_entry)
            symbol_data = parsed_entry.get(symbol, {})
            return symbol_data.get("imbalancePrice", 0)
        except Exception as e:
            return 0
    data['deliveryStart'] = pd.to_datetime(data['deliveryStart'])

    data.set_index('deliveryStart', inplace=True)

    # Apply the extraction function to each row in the 'entryPerArea' column for the specified symbol
    data[f'{symbol}_acceptedUpBidVolume'] = data['entryPerArea'].apply(extract_symbol_data)
    
    # Calculating hourly percentage change for the specified symbol
    data[f'{symbol}_acceptedUpBidVolume_pct_change'] = data[f'{symbol}_acceptedUpBidVolume'].pct_change().fillna(0) * 100
    
    # Resample by hour to get average percentage change per hour
    hourly_avg_change = data[f'{symbol}_acceptedUpBidVolume_pct_change'].resample('H').mean()

    plt.figure(figsize=(14, 7))
    hourly_avg_change.plot()
    plt.title(f'Hourly Percentage Change in imbalancePrice for {symbol}')
    plt.ylabel('% Change')
    plt.xlabel('Time')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    return plt

# Test the function with 'DK1' to ensure it works as expected
# Streamlit app starts here
st.title('Production vs Consumption Visualization')

# Assuming 'production.csv' and 'consumption.csv' are in the same directory as your app
# If not, adjust the paths accordingly

production_data = pd.read_csv('/home/dias/production.csv')
consumption_data = pd.read_csv('/home/dias/consumption.csv')
balance_data = pd.read_csv('/home/dias/balance.csv')
# Streamlit interface to select symbol and date
symbol = st.selectbox('Select the symbol:', production_data['symbol'].unique())
date = st.date_input('Select the date:')

# Assuming 'df' is your DataFrame variable that contains the data.
if st.button('Show Plots'):

    fig = plot_energy_sources_pie_with_other(production_data, symbol, str(date))
    st.pyplot(fig)
    fig = plot_production_vs_consumption_revised(production_data, consumption_data, date.strftime('%Y-%m-%d'), symbol)
    st.pyplot(fig)
    fig = plot_percentage_change_for_symbol(symbol,balance_data)
    st.pyplot(fig)
