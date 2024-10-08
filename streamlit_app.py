import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import json
import re
from collections import Counter

# Disable math text parsing globally
mpl.rcParams['text.usetex'] = False
mpl.rcParams['mathtext.default'] = 'regular'

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

# Function to extract versions from an item
def extract_versions(item):
    versions = []
    if 'version' in item:
        versions.append(item['version'])
    if 'prebidInstances' in item:
        for instance in item['prebidInstances']:
            if 'version' in instance:
                versions.append(instance['version'])
    return versions

# Function to extract modules from an item
def extract_modules(item):
    modules = []
    if 'modules' in item:
        modules.extend(item['modules'])
    if 'prebidInstances' in item:
        for instance in item['prebidInstances']:
            if 'modules' in instance:
                modules.extend(instance['modules'])
    return modules

# Function to count total modules in an item
def count_modules(item):
    modules = extract_modules(item)
    return len(modules)

# Function to count Prebid instances in an item
def count_prebid_instances(item):
    if 'prebidInstances' in item:
        return len(item['prebidInstances'])
    elif 'version' in item:
        return 1
    else:
        return 0

# Function to extract libraries from an item
def extract_libraries(item):
    libraries = []
    if 'libraries' in item:
        libraries.extend(item['libraries'])
    return libraries

# Function to extract global variable names from data
def extract_global_var_names(data):
    global_var_names = []
    for item in data:
        if 'prebidInstances' in item:
            for instance in item['prebidInstances']:
                if 'globalVarName' in instance:
                    global_var_names.append(instance['globalVarName'])
        else:
            # If 'prebidInstances' is not present but 'globalVarName' is at top level
            if 'globalVarName' in item:
                global_var_names.append(item['globalVarName'])
    return global_var_names

# Function to extract and classify modules
def extract_module_stats(data):
    module_site_counter = {
        'Bid Adapter': Counter(),
        'RTD Module': Counter(),
        'ID System': Counter(),
        'Analytics Adapter': Counter(),
        'Other': Counter()
    }

    module_instance_counter = {
        'Bid Adapter': Counter(),
        'RTD Module': Counter(),
        'ID System': Counter(),
        'Analytics Adapter': Counter(),
        'Other': Counter()
    }

    for item in data:
        modules = extract_modules(item)
        unique_modules = set(modules)  # Unique modules in this site

        # Count number of sites per module
        for module in unique_modules:
            category = classify_module(module)
            module_site_counter[category][module] += 1

        # Count total instances per module
        for module in modules:
            category = classify_module(module)
            module_instance_counter[category][module] += 1

    return module_site_counter, module_instance_counter

# Create a bar chart of the version buckets
def create_version_chart(data):
    # Extract and categorize versions
    version_buckets = []
    for item in data:
        versions = extract_versions(item)
        for version in versions:
            version_bucket = categorize_version(version)
            version_buckets.append(version_bucket)

    # Create a DataFrame and count occurrences of each version bucket
    version_counts = pd.Series(version_buckets).value_counts().sort_index()

    # Plot the bar chart
    fig, ax = plt.subplots()
    ax.bar(version_counts.index, version_counts.values)
    ax.set_xlabel('Version Buckets')
    ax.set_ylabel('Number of URLs')
    ax.set_title('Number of URLs per Version Bucket')
    plt.xticks(rotation=45, ha='right')
    st.pyplot(fig)

    # Display the total number of sites
    st.write(f"Total Number of Sites: {len(data)}")

# Create a bar chart of Prebid instances per site
def create_prebid_instance_chart(data):
    prebid_instance_counts = [count_prebid_instances(item) for item in data]
    
    # Adjust labels and bins to include zero instances
    labels = ['0', '1', '2', '3', '4', '5', '6+']
    bins = [-0.1, 0,1,2,3,4,5,float('inf')]  # Start from -0.1 to include zero counts properly
    
    binned_counts = pd.cut(prebid_instance_counts, bins=bins, right=True, labels=labels)
    prebid_instance_distribution = binned_counts.value_counts().sort_index()
    
    # Plot the bar chart
    fig, ax = plt.subplots()
    ax.bar(prebid_instance_distribution.index.astype(str), prebid_instance_distribution.values)
    ax.set_xlabel('Number of Prebid Instances per Site')
    ax.set_ylabel('Number of Sites')
    ax.set_title('Distribution of Prebid Instances per Site')
    plt.xticks(rotation=0)
    st.pyplot(fig)

# Create a bar chart of library popularity
def create_library_chart(data):
    all_libraries = []
    for item in data:
        libraries = extract_libraries(item)
        all_libraries.extend(libraries)
    
    if all_libraries:
        library_counts = pd.Series(all_libraries).value_counts().sort_values(ascending=False)

        # Escape special characters in labels
        def escape_label(label):
            special_chars = ['_', '$', '%', '&', '#', '{', '}', '~', '^', '\\']
            for char in special_chars:
                label = label.replace(char, f'\\{char}')
            return label

        escaped_labels = [escape_label(name) for name in library_counts.index]

        # Plot the bar chart using Matplotlib directly
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(range(len(library_counts)), library_counts.values)
        ax.set_xlabel('Libraries')
        ax.set_ylabel('Number of Sites')
        ax.set_title('Popularity of Detected Libraries')

        # Set x-axis labels
        ax.set_xticks(range(len(escaped_labels)))
        ax.set_xticklabels(escaped_labels, rotation=45, ha='right')

        # Use FixedFormatter to prevent automatic formatting
        import matplotlib.ticker as ticker
        ax.xaxis.set_major_formatter(ticker.FixedFormatter(escaped_labels))

        # Ensure labels are treated as plain text
        for label in ax.get_xticklabels():
            label.set_text(label.get_text())

        st.pyplot(fig)
    else:
        st.write("No libraries data available to plot.")

# Create a bar chart of Prebid global object name popularity
def create_global_var_name_chart(data):
    import matplotlib.ticker as ticker
    
    global_var_names = extract_global_var_names(data)
    if global_var_names:
        # Count occurrences of each global variable name
        global_var_name_counts = pd.Series(global_var_names).value_counts().sort_values(ascending=False)
        
        # Escape special characters in labels
        def escape_label(label):
            special_chars = ['_', '$', '%', '&', '#', '{', '}', '~', '^', '\\']
            for char in special_chars:
                label = label.replace(char, f'\\{char}')
            return label
        
        escaped_labels = [escape_label(name) for name in global_var_name_counts.index]
        
        # Plot the bar chart using Matplotlib directly
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(range(len(global_var_name_counts)), global_var_name_counts.values)
        ax.set_xlabel('Prebid Global Object Names')
        ax.set_ylabel('Number of Sites')
        ax.set_title('Popularity of Prebid Global Object Names')
        
        # Set x-axis labels with proper alignment
        ax.set_xticks(range(len(escaped_labels)))
        ax.set_xticklabels(escaped_labels, rotation=45, ha='right')
        
        # Use FixedFormatter to prevent automatic formatting
        ax.xaxis.set_major_formatter(ticker.FixedFormatter(escaped_labels))
        
        # Ensure labels are treated as plain text
        for label in ax.get_xticklabels():
            label.set_text(label.get_text())
        
        st.pyplot(fig)
    else:
        st.write("No Prebid global variable names found to plot.")

# Function to display module statistics
def display_module_stats(module_site_stats, module_instance_stats, total_sites):
    for category in module_site_stats.keys():
        site_counter = module_site_stats[category]
        instance_counter = module_instance_stats[category]

        # Create a DataFrame with columns: Module Name, Number of Sites, Number of Instances
        df = pd.DataFrame({
            category: list(site_counter.keys()),
            'Number of Sites': list(site_counter.values()),
            'Number of Instances': [instance_counter[module] for module in site_counter.keys()]
        })

        # Sort the DataFrame by Number of Sites
        df = df.sort_values(by='Number of Sites', ascending=False).reset_index(drop=True)

        # Compute total instances for this category
        total_instances = sum(instance_counter.values())

        # Display total number of sites and instances for reference
        st.subheader(f"{category} Popularity (Total Sites: {total_sites}, Total Instances: {total_instances})")
        st.table(df)

# Streamlit app
st.title('Prebid Analysis Dashboard')

uploaded_file = st.file_uploader('Upload a JSON file', type='json')

if uploaded_file is not None:
    data = load_json(uploaded_file)
    if data:  # Proceed only if there is valid data
        # Filter out entries with more than 300 modules
        filtered_data = [item for item in data if count_modules(item) <= 300]
        total_sites = len(filtered_data)  # Total number of sites after filtering
        create_version_chart(filtered_data)
        create_prebid_instance_chart(filtered_data)
        create_library_chart(filtered_data)
        create_global_var_name_chart(filtered_data)
        module_site_stats, module_instance_stats = extract_module_stats(filtered_data)
        display_module_stats(module_site_stats, module_instance_stats, total_sites)
    else:
        st.write("No valid data found in the uploaded file.")
