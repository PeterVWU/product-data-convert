import pandas as pd
import os
import re

# Input CSV from Magento
input_filename = "magento_cleaned_aggregated.csv"
df = pd.read_csv(input_filename)

# Modify categ_id to include "All / " prefix
df["categ_id"] = "All / " + df["attribute_set_code"].fillna("Default").astype(str)

# Extract all unique categories
output_dir = "output"
categories_dir = os.path.join(output_dir, "categories")
os.makedirs(categories_dir, exist_ok=True)
categories_filename = os.path.join(categories_dir, "odoo_categories.csv")
unique_categories = df["categ_id"].unique()
pd.DataFrame({"category_name": unique_categories}).to_csv(categories_filename, index=False)

# Convert numeric columns to integers, handling nulls and strings
def safe_convert_to_int(value):
    try:
        if pd.isna(value) or value == '':
            return 0
        # First convert to float (to handle decimal strings) then to int
        return int(float(value))
    except (ValueError, TypeError):
        return 0

# Apply integer conversion to the specific columns
df['puff_counts'] = df['puff_counts'].apply(safe_convert_to_int)
df['volume'] = df['volume'].apply(safe_convert_to_int)
df['nicotine_level'] = df['nicotine_level'].apply(safe_convert_to_int)

odoo_df = pd.DataFrame({
    "default_code": df["sku"],
    "name": df["name"],
    "categ_id": df["categ_id"],
    "list_price": df["price"],
    "standard_price": df["cost"],
    "type": "goods", 
    "barcode": df["upc"],
    "weight": df["weight"],
    "x_puff_count": df["puff_counts"].astype(int),
    "x_flavor": df["flavor"],
    "x_volume": df["volume"].astype(int),
    "x_nicotine_level": df["nicotine_level"].astype(int),
    "x_pack_size": df["pack_size"],
    "x_brand": df["brand"],
    "x_resistance": df["resistance"], 
})

# Function to split and save DataFrame in chunks
def split_and_save_csv(df, folder_name, base_filename, chunk_size=1999):
    folder_path = os.path.join(output_dir, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    for i, chunk in enumerate(range(0, len(df), chunk_size)):
        chunk_filename = os.path.join(folder_path, f"{base_filename}_part_{i+1}.csv")
        df.iloc[chunk:chunk + chunk_size].to_csv(chunk_filename, index=False)
        print(f"CSV chunk created: {chunk_filename}")

# Save the updated DataFrame in smaller chunks
split_and_save_csv(odoo_df, "products", "odoo_import")

# Process Bill of Materials (BoM) import only for multi-pack products with single product SKUs
# Filter to include only:
# 1. Products with pack_size > 1 (multi-packs)
# 2. Products that have a valid single_product_sku (not empty)
# 3. Products whose single_product_sku exists in our valid components list
valid_components = df["sku"].unique()
bom_df = df[
    (df['pack_size'] > 1) & 
    (df['single_product_sku'].notna()) & 
    (df['single_product_sku'].str.strip() != '') &
    (df['single_product_sku'].isin(valid_components))
]

# Create a set of product SKUs that have BoM relationships
products_with_bom = set()

if len(bom_df) > 0:
    # Add all multi-pack SKUs with valid BoM to the set
    products_with_bom.update(bom_df['sku'].tolist())
    odoo_bom = pd.DataFrame({
        "product": bom_df['sku'],
        "BoM Type": "Kit",
        "BoM Lines/Component": bom_df['single_product_sku'],
        "BoM Lines/Quantity": bom_df['pack_size']
    })
    split_and_save_csv(odoo_bom, "bom", "odoo_bom_import")
    print(f"Created BoM import files for {len(bom_df)} multi-pack products")
else:
    print("No valid multi-pack products with single product SKUs found for BoM creation")

# Process location format (G-4-1) into hierarchical structure (WH / G / 4 / 1)
def parse_location(location_str):
    # Check if location matches the pattern like "G-4-1"
    pattern = r'^([A-Za-z])-(\d+)-(\d+)$'
    match = re.match(pattern, location_str.strip())
    
    if match:
        row = match.group(1).upper()  # G (row of shelf)
        column = match.group(2)       # 4 (column)
        level = match.group(3)        # 1 (shelf level)
        
        # Return structured location path and components
        return {
            'path': f"WH/{row}/{column}/{level}",
            'row': row,
            'column': column,
            'level': level
        }
    else:
        # If location doesn't match expected format, place directly under WH
        return {
            'path': f"WH/{location_str.strip()}",
            'row': None,
            'column': None,
            'level': None
        }

# Parse the locations string into separate rows for each location
def expand_locations(row):
    sku = row.get('sku', '')
     # Skip products with BoM (multi-packs with single product relationships)
    if sku in products_with_bom:
        return []
        
    locations_str = row.get('locations', '')
    if pd.isna(locations_str) or str(locations_str).strip() == '':
        return []
    
    expanded_rows = []
    for loc_qty in locations_str.split(';'):
        if ':' not in loc_qty:
            continue
        location, qty = loc_qty.split(':')
        parsed_location = parse_location(location)
        expanded_rows.append({
            'product': row['sku'],
            'location': parsed_location['path'],
            'inventoried_quantity': int(qty),
            'original_location': location.strip()
        })
    return expanded_rows

# Create expanded inventory rows with locations
inventory_rows = []
for _, row in df.iterrows():
    location_rows = expand_locations(row)
    inventory_rows.extend(location_rows)
    
# Convert to DataFrame
location_inventory_df = pd.DataFrame(inventory_rows)

# Filter out products with BoM relationships from the products_without_locations
products_without_locations = df[
    ((df['locations'].isna()) | (df['locations'] == '')) &
    (~df['sku'].isin(products_with_bom))  # Exclude products with BoM
]


if not products_without_locations.empty:
    default_location_rows = pd.DataFrame({
        'product': products_without_locations['sku'],
        'location': 'WH/Stock',  # Default Odoo location
        'inventoried_quantity': products_without_locations['qty'],
        'original_location': 'Stock'
    })
    if len(location_inventory_df) > 0:
        location_inventory_df = pd.concat([location_inventory_df, default_location_rows])
    else:
        location_inventory_df = default_location_rows


# Sort and reset index for inventory
if len(location_inventory_df) > 0:
    odoo_inventory_qty = location_inventory_df[['product', 'location', 'inventoried_quantity']]
    odoo_inventory_qty = odoo_inventory_qty.sort_values(['product', 'location']).reset_index(drop=True)
    
    # Add a note about products excluded due to BoM
    print(f"\nInventory Import Summary:")
    print(f"Total products with inventory: {len(odoo_inventory_qty['product'].unique())}")
    print(f"Total products excluded due to BoM relationships: {len(products_with_bom)}")

    # Process inventory quantity import
    split_and_save_csv(odoo_inventory_qty, "inventory", "odoo_inventory_qty_import")
else:
    print("No inventory data found.")

# Process locations for hierarchical structure
all_locations = {'WH': {'type': 'view', 'parent': None, 'level': 0}}

# Process all locations to build the hierarchy
if len(location_inventory_df) > 0:
    for _, row in location_inventory_df.iterrows():
        location_path = row['location']
        
        # Skip the default location which is already handled
        if location_path == 'WH/Stock':
            all_locations['WH/Stock'] = {'type': 'internal', 'parent': 'WH', 'level': 1}
            continue
            
        # Split the path into components
        parts = location_path.split('/')
        
        # Skip invalid paths
        if len(parts) < 2:
            continue
            
        # Process each level of the path to ensure all parent locations exist
        for i in range(2, len(parts) + 1):
            current_path = '/'.join(parts[:i])
            parent_path = '/'.join(parts[:i-1])
            
            if current_path not in all_locations:
                # Determine location type (internal for leaf nodes, view for parent nodes)
                loc_type = 'internal'
                all_locations[current_path] = {'type': loc_type, 'parent': parent_path, 'level': i-1}

# Create location records for import
location_records = []
for path, details in all_locations.items():
    # Special case for the root warehouse
    if path == 'WH':
        continue  # Skip WH as it already exists in Odoo
        
    name = path.split('/')[-1]  # Last component of the path is the location name
    location_records.append({
        'name': name,
        'location_type': details['type'],
        'parent_location': details['parent'],
        'posx': 0,  # Default position values
        'posy': 0,
        'posz': 0,
        'level': details['level']
    })

# Sort locations by level to ensure parent locations are created first
location_records.sort(key=lambda x: x['level'])


# Create DataFrame for locations import and split by hierarchy level
if location_records:
    # Group locations by their hierarchy level
    locations_by_level = {}
    for record in location_records:
        level = record['level']
        if level not in locations_by_level:
            locations_by_level[level] = []
        locations_by_level[level].append(record)
    
    # Create a directory for locations
    locations_dir = os.path.join(output_dir, "locations")
    os.makedirs(locations_dir, exist_ok=True)
    
    # Create separate CSV files for each level
    total_locations = 0
    for level, records in sorted(locations_by_level.items()):
        # Create DataFrame for this level
        level_df = pd.DataFrame(records)
        
        # Reorder columns for clarity and remove the level column which is only for sorting
        level_df = level_df[['name', 'location_type', 'parent_location', 'posx', 'posy', 'posz']]
        
        # Save this level to its own CSV file
        level_filename = os.path.join(locations_dir, f"odoo_locations_level_{level}.csv")
        level_df.to_csv(level_filename, index=False)
        
        total_locations += len(records)
        print(f"Level {level} locations saved to: {level_filename} ({len(records)} locations)")
    
    # Also create a combined file for reference
    all_locations_df = pd.DataFrame(location_records)
    all_locations_df = all_locations_df[['name', 'location_type', 'parent_location', 'posx', 'posy', 'posz', 'level']]
    combined_filename = os.path.join(locations_dir, "odoo_locations_all.csv")
    all_locations_df.to_csv(combined_filename, index=False)
    
    print(f"All locations combined in reference file: {combined_filename}")
    print(f"Total locations: {total_locations}")
    print(f"IMPORTANT: Import location files in level order, from lowest level number to highest")
else:
    print("No location data found.")

print(f"Categories extracted and saved to: {categories_filename}")