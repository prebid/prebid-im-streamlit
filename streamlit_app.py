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
    module_name_lower = module_name.lower()
    if 'bidadapter' in module_name_lower or 'bidadapter' in module_name_lower:
        return 'Bid Adapter'
    elif 'rtdprovider' in module_name_lower or 'rtdmodule' in module_name_lower:
        return 'RTD Module'
    elif 'idsystem' in module_name_lower or 'userid' in module_name_lower:
        return 'ID System'
    elif 'analytics' in module_name_lower or 'analyticsadapter' in module_name_lower:
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
        modules_list = []
        if 'prebidInstances' in item:
            prebid_instances = item.get('prebidInstances', [])
            for instance in prebid_instances:
                modules = instance.get('modules', [])
                modules_list.extend(modules)
        else:
            # Fallback to prior data structure
            modules = item.get('modules', [])
            modules_list.extend(modules)
        
        for module in modules_list:
            category = classify_module(module)
            module_counter[category][module] += 1

    return module_counter

# Create a bar chart of the version buckets
def create_version_chart(data):
    version_buckets = []

    for item in data:
        if 'prebidInstances' in item:
            prebid_instances = item.get('prebidInstances', [])
            for instance in prebid_instances:
                version = instance.get('version', '')
                if version:
                    bucket = categorize_version(version)
                    version_buckets.append(bucket)
        else:
            # Fallback to prior data structure
            version = item.get('version', '')
            if version:
                bucket = categorize_version(version)
                version_buckets.append(bucket)

    # Create a DataFrame and count occurrences of each version bucket
    if version_buckets:
        version_counts = pd.Series(version_buckets).value_counts().sort_index()

        # Plot the bar chart
        fig, ax = plt.subplots()
        version_counts.plot(kind='bar', ax=ax)
        ax.set_xlabel('Version Buckets')
        ax.set_ylabel('Number of Instances')
        ax.set_title('Number of Prebid.js Instances per Version Bucket')
        plt.xticks(rotation=45)
        st.pyplot(fig)

        # Display the total number of instances
        st.write(f"Total Number of Prebid.js Instances: {len(version_buckets)}")
    else:
        st.write("No Prebid.js version information available.")

# Function to display module statistics
def display_module_stats(module_stats):
    for category, counter in module_stats.items():
        st.subheader(f"{category} Popularity")
        df = pd.DataFrame(counter.items(), columns=[category, 'Count'])
        df = df.sort_values(by='Count', ascending=False).reset_index(drop=True)
        st.table(df)

# Function to create a plot for the popularity of other libraries
def create_libraries_chart(data):
    libraries_list = []

    for item in data:
        libraries = item.get('libraries', [])
        libraries_list.extend(libraries)

    if libraries_list:
        library_counts = pd.Series(libraries_list).value_counts().sort_values(ascending=False)

        # Plot the bar chart
        fig, ax = plt.subplots()
        library_counts.plot(kind='bar', ax=ax)
        ax.set_xlabel('Libraries')
        ax.set_ylabel('Number of URLs')
        ax.set_title('Popularity of Other Libraries Detected')
        plt.xticks(rotation=45)
        st.pyplot(fig)

        # Display the total number of URLs
        st.write(f"Total Number of URLs: {len(data)}")
    else:
        st.write("No libraries information available.")

# Streamlit app
st.title('Prebid.js and Libraries Analysis')

uploaded_file = st.file_uploader('Upload a JSON file', type='json')

if uploaded_file is not None:
    data = load_json(uploaded_file)
    if data:  # Proceed only if there is valid data
        # Filter out entries with more than 300 modules in any instance
        filtered_data = []
        for item in data:
            include_item = True
            modules_list = []
            if 'prebidInstances' in item:
                prebid_instances = item.get('prebidInstances', [])
                for instance in prebid_instances:
                    if len(instance.get('modules', [])) > 300:
                        include_item = False
                        break
                    modules_list.extend(instance.get('modules', []))
            else:
                # Fallback to prior data structure
                if len(item.get('modules', [])) > 300:
                    include_item = False
                modules_list.extend(item.get('modules', []))
            if include_item:
                filtered_data.append(item)

        st.header('Version Popularity Chart (Grouped by Buckets)')
        create_version_chart(filtered_data)

        st.header('Module Statistics')
        module_stats = extract_module_stats(filtered_data)
        display_module_stats(module_stats)

        st.header('Popularity of Other Libraries Detected')
        create_libraries_chart(filtered_data)
    else:
        st.write("No valid data found in the uploaded file.")
