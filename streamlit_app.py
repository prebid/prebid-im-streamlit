import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import json
import re
from collections import Counter

# Load the JSON data from the uploaded file
def load_json(file):
    try:
        # Load JSON data into a Python list
        data = json.load(file)
    except json.JSONDecodeError:
        st.error("The uploaded file is not a valid JSON.")
        return None
    return data

# Function to categorize versions into buckets
def categorize_version(version):
    # Remove leading 'v' if present
    if version.startswith('v'):
        version = version[1:]

    # Split the version into major and minor parts
    version_parts = re.split(r'\.|-', version)
    
    try:
        major = int(version_parts[0])
        minor = int(version_parts[1]) if len(version_parts) > 1 else 0
    except ValueError:
        return 'Other'

    # Group versions based on major and minor version numbers
    if major == 8:
        if 0 <= minor <= 9:
            return '8.0-8.9'
        elif 10 <= minor <= 19:
            return '8.10-8.19'
        elif 20 <= minor <= 29:
            return '8.20-8.29'
        elif 30 <= minor <= 39:
            return '8.30-8.39'
        elif 40 <= minor <= 49:
            return '8.40-8.49'
        elif 50 <= minor <= 59:
            return '8.50-8.59'
        else:
            return '8.60+'
    elif major == 9:
        if 0 <= minor <= 9:
            return '9.0-9.9'
        elif 10 <= minor <= 19:
            return '9.10-9.19'
        elif 20 <= minor <= 29:
            return '9.20-9.29'
        elif 30 <= minor <= 39:
            return '9.30-9.39'
        elif 40 <= minor <= 49:
            return '9.40-9.49'
        elif 50 <= minor <= 59:
            return '9.50-9.59'
        else:
            return '9.60+'
    elif major == 7:
        if 0 <= minor <= 9:
            return '7.0-7.9'
        elif 10 <= minor <= 19:
            return '7.10-7.19'
        elif 20 <= minor <= 29:
            return '7.20-7.29'
        elif 30 <= minor <= 39:
            return '7.30-7.39'
        elif 40 <= minor <= 49:
            return '7.40-7.49'
        elif 50 <= minor <= 59:
            return '7.50-7.59'
        else:
            return '7.60+'
    elif major in range(3, 7):
        return f'{major}.x'
    elif major in range(0, 3):
        return '0.x-2.x'
    else:
        return 'Other'

# Classify modules by type
def classify_module(module_name):
    if 'BidAdapter' in module_name:
        return 'Bid Adapter'
    elif 'RtdProvider' in module_name or 'rtdModule' in module_name:
        return 'RTD Module'
    elif 'IdSystem' in module_name or 'userId' in module_name:
        return 'ID System'
    elif 'Analytics' in module_name or 'analyticsAdapter' in module_name:
        return 'Analytics Adapter'
    else:
        return 'Other'

# Function to extract and classify modules
def extract_module_stats(data):
    module_counter = {
        'Bid Adapter': Counter(),
        'RTD Module': Counter(),
        'ID System': Counter(),
        'Analytics Adapter': Counter(),
        'Other': Counter()
    }

    for item in data:
        # Safely get the 'modules' list or return an empty list if the key is missing
        modules = item.get('modules', [])
        for module in modules:
            category = classify_module(module)
            module_counter[category][module] += 1

    return module_counter

# Create a bar chart of the version buckets
def create_version_chart(data):
    # Extract and categorize versions
    version_buckets = [categorize_version(item['version']) for item in data]
    
    # Create a DataFrame and count occurrences of each version bucket
    version_counts = pd.Series(version_buckets).value_counts().sort_index()

    # Plot the bar chart
    fig, ax = plt.subplots()
    version_counts.plot(kind='bar', ax=ax)
    ax.set_xlabel('Version Buckets')
    ax.set_ylabel('Number of URLs')
    ax.set_title('Number of URLs per Version Bucket')
    plt.xticks(rotation=45)
    st.pyplot(fig)

    # Display the total number of sites
    st.write(f"Total Number of Sites: {len(data)}")

# Function to display module statistics
def display_module_stats(module_stats):
    for category, counter in module_stats.items():
        st.subheader(f"{category} Popularity")
        df = pd.DataFrame(counter.items(), columns=[category, 'Count'])
        df = df.sort_values(by='Count', ascending=False).reset_index(drop=True)
        st.table(df)

# Streamlit app
st.title('Version Popularity Chart (Grouped by Buckets)')

uploaded_file = st.file_uploader('Upload a JSON file eg https://github.com/prebid/prebid-integration-monitor/blob/main/output/10k.json', type='json')

if uploaded_file is not None:
    data = load_json(uploaded_file)
    if data:  # Proceed only if there is valid data
        # Filter out entries with more than 300 modules
        filtered_data = [item for item in data if len(item.get('modules', [])) <= 300]
        create_version_chart(filtered_data)
        module_stats = extract_module_stats(filtered_data)
        display_module_stats(module_stats)
    else:
        st.write("No valid data found in the uploaded file.")
