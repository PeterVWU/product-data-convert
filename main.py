import csv
import re

def extract_pack_size(product_name, unit_per_pack_str):
    """
    Extract the pack size from the product name or unit_per_pack field.
    Uses a simple regex heuristic.
    """
    # Explicit check for common single indicators.
    if unit_per_pack_str.strip().lower() in ['single disposable', 'single', 'one']:
        return 1

    # Try to extract using the product_name first.
    match = re.search(r'(\d+)[- ]?[Pp]ack', product_name)
    if match:
        return int(match.group(1))
    
    # Fallback: search the unit_per_pack_str.
    if unit_per_pack_str:
        found = re.search(r'(\d+)', unit_per_pack_str)
        if found:
            return int(found.group(1))
    
    # Default to 1 if nothing found.
    return 1

def load_duoplane_mapping(duoplane_csv_file):
    """
    Load the DuoPlane product list and create a mapping from vendor SKUs to retail SKUs.
    Only includes products from vendor "NV01".
    
    Args:
        duoplane_csv_file (str): Path to the DuoPlane CSV file
        
    Returns:
        dict: Mapping from vendor SKU to retail SKU
    """
    vendor_to_retail_sku_map = {}
    
    with open(duoplane_csv_file, mode='r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            vendor_name = row.get('vendor_name', '').strip()
            
            # Only process rows for vendor "NV01"
            if vendor_name == 'NV01':
                vendor_sku = row.get('vendor_sku', '').strip()
                retail_sku = row.get('retailer_sku', '').strip()
                
                if vendor_sku and retail_sku:
                    vendor_to_retail_sku_map[vendor_sku] = retail_sku
    
    print(f"Loaded {len(vendor_to_retail_sku_map)} SKU mappings from vendor NV01")
    return vendor_to_retail_sku_map


def generate_canonical_name(base_name, category, brand, volume, nicotine_level, flavor, resistance):
    """
    Build a canonical name using the provided base name and additional attributes.
    Include category and brand to better differentiate products.
    """
    base_name = base_name.strip().lower()
    category = category.strip().lower() if category else ""
    brand = brand.strip().lower() if brand else ""
    
    # Start with category + brand + base name for more precise differentiation
    canonical = f"{category}_{brand}_{base_name}" if category and brand else base_name
    
    attrs = []
    for attr in [str(volume), str(nicotine_level), flavor, resistance]:
        attr_str = attr.strip().lower() if attr else ""
        if attr_str and attr_str not in ['0', '0.0']:
            attrs.append(attr_str)
    
    if attrs:
        canonical += " | " + " | ".join(attrs)
    return canonical

def should_generate_canonical_name(volume, nicotine_level, flavor, resistance):
    """
    Determine if a product has enough data to generate a meaningful canonical name.
    Returns True if at least one attribute has a value, False otherwise.
    """
    has_value = False
    for attr in [str(volume), str(nicotine_level), flavor, resistance]:
        attr_str = attr.strip().lower() if attr else ""
        if attr_str and attr_str not in ['0', '0.0']:
            has_value = True
            break
    return has_value
    

def load_inventory_data(inventory_csv_file, duoplane_csv_file):
    """
    Modified version of load_inventory_data that uses the DuoPlane mapping.
    Maps vendor SKUs to retail SKUs before processing.
    Handles duplicate UPCs by modifying all but one of the duplicates.
    
    Args:
        inventory_csv_file (str): Path to the inventory CSV file
        duoplane_csv_file (str): Path to the DuoPlane CSV file
        
    Returns:
        tuple: (inventory_sku_map, duplicate_count, duplicate_skus)
    """
    # Load the DuoPlane mapping
    vendor_to_retail_map = load_duoplane_mapping(duoplane_csv_file)
    
    inventory_sku_map = {}
    duplicate_count = 0
    duplicate_skus = []
    unmapped_skus = []
    
    # First pass: collect all rows and track UPCs
    upc_to_sku_map = {}
    duplicate_upcs = set()
    
    # Temporary storage for all rows
    inventory_rows = []

    with open(inventory_csv_file, mode='r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        for row_index, row in enumerate(reader):
            vendor_sku = row.get('SKU', '').strip()
            if not vendor_sku:
                continue  # Skip if SKU is empty

            # Map the vendor SKU to retail SKU
            retail_sku = vendor_to_retail_map.get(vendor_sku)
            
            # If no mapping found, track it and use the vendor SKU directly
            if not retail_sku:
                unmapped_skus.append(vendor_sku)
                retail_sku = vendor_sku
            
            # Use default values for numeric fields to prevent skipping rows
            try:
                cost = float(row.get('Cost', '0').strip() or '0')
            except ValueError:
                cost = 0.0
                
            try:
                price = float(row.get('Price', '0').strip() or '0')
            except ValueError:
                price = 0.0
                
            try:
                total_qty = int(float(row.get('Quantity', '0').strip() or '0'))
            except ValueError:
                total_qty = 0
                
            try:
                location_qty = int(float(row.get('Qty_1', '0').strip() or '0'))
            except ValueError:
                location_qty = 0
                
            try:
                weight = float(row.get('Weight', '0').strip() or '0')
            except ValueError:
                weight = 0.0

            location = row.get('Location_1', '').strip()
            upc = row.get('UPC', '').strip()
            
            # Track UPC usage
            if upc:
                if upc in upc_to_sku_map:
                    duplicate_upcs.add(upc)
                else:
                    upc_to_sku_map[upc] = vendor_sku
            
            # Store the row for processing after we've identified all duplicate UPCs
            inventory_rows.append({
                'vendor_sku': vendor_sku,
                'retail_sku': retail_sku,
                'cost': cost,
                'price': price,
                'total_qty': total_qty,
                'location_qty': location_qty,
                'weight': weight,
                'location': location,
                'upc': upc,
                'row_index': row_index
            })

    # Report on duplicate UPCs
    if duplicate_upcs:
        print(f"\nFound {len(duplicate_upcs)} duplicate UPCs. These will be fixed.")
        for upc in duplicate_upcs:
            print(f"  - UPC {upc} is used by multiple products.")
    
    # Second pass: process rows and handle duplicate UPCs
    for row in inventory_rows:
        vendor_sku = row['vendor_sku']
        retail_sku = row['retail_sku']
        upc = row['upc']
        
        # Handle duplicate UPCs - keep the original for the first product,
        # add a suffix digit for others
        if upc in duplicate_upcs:
            # Is this the first occurrence of this UPC we're keeping unchanged?
            if upc_to_sku_map.get(upc) == vendor_sku:
                # This is the first product with this UPC, keep it unchanged
                pass
            else:
                # Find all SKUs with this UPC to determine suffix number
                conflict_count = 1
                for other_row in inventory_rows:
                    if other_row['upc'] == upc and other_row['vendor_sku'] == upc_to_sku_map.get(upc):
                        # This is the original product with this UPC
                        continue
                    if other_row['upc'] == upc and other_row['vendor_sku'] != vendor_sku and other_row['row_index'] < row['row_index']:
                        # This is another product with the same UPC that we've already processed
                        conflict_count += 1
                
                # Append the conflict count to make the UPC unique
                original_upc = upc
                upc = f"{upc}{conflict_count}"
                print(f"  - Changed UPC for {vendor_sku} from {original_upc} to {upc}")
        
        # Additional product details
        product_details = {
            'cost': row['cost'],
            'price': row['price'],
            'qty': max(row['total_qty'], 0),  # Ensure non-negative
            'weight': row['weight'],
            'upc': upc,  # Use potentially modified UPC
            'retail_sku': retail_sku,
            'locations': {},  # Dictionary to store location-specific quantities
            'row_index': row['row_index']  # Track which row this came from
        }

        if vendor_sku in inventory_sku_map:
            # This is a duplicate SKU
            duplicate_count += 1
            duplicate_skus.append(vendor_sku)

            # Update existing SKU's locations
            if row['location']:  # Only require location, not qty > 0
                inventory_sku_map[vendor_sku]['locations'][row['location']] = max(row['location_qty'], 0)
                
            # Keep track of the total quantity across all duplicates
            inventory_sku_map[vendor_sku]['qty'] += max(row['total_qty'], 0)
        else:
            # Create new SKU entry regardless of location/qty
            if row['location']:
                product_details['locations'][row['location']] = max(row['location_qty'], 0)
            inventory_sku_map[vendor_sku] = product_details  # Always add the SKU

    print(f"Total unique SKUs after mapping: {len(inventory_sku_map)}")
    print(f"Total duplicate SKUs: {duplicate_count}")
    print(f"Number of unique duplicate SKUs: {len(set(duplicate_skus))}")
    print(f"Number of unmapped vendor SKUs: {len(unmapped_skus)}")
    
    # Print the first 20 unmapped SKUs (or all if less than 20)
    if unmapped_skus:
        print("\nSample of unmapped vendor SKUs:")
        for sku in sorted(set(unmapped_skus))[:20]:
            print(f"  - {sku}")
    
    # Print the first 20 duplicate SKUs (or all if less than 20)
    if duplicate_skus:
        print("\nSample of duplicate SKUs:")
        for sku in sorted(set(duplicate_skus))[:20]:
            print(f"  - {sku}")
    
    return inventory_sku_map, duplicate_count, duplicate_skus

def clean_and_aggregate_magento_csv(input_file, output_file, inventory_csv_file, duoplane_csv_file):
    # Load the extra CSV with correct cost and quantity, using the DuoPlane mapping
    inventory_sku_map, duplicate_count, duplicate_skus = load_inventory_data(inventory_csv_file, duoplane_csv_file)
    
    print(f"\nInventory Summary:")
    print(f"Total rows in inventory: {len(inventory_sku_map) + duplicate_count}")
    print(f"Total unique SKUs: {len(inventory_sku_map)}")

    # Define the columns to keep from the CSV.
    columns_to_keep = [
        'sku',
        'attribute_set_code',
        'unit_per_pack',
        'product_type',
        'name',
        'weight',
        'product_online',
        'visibility',
        'price',
        'base_image',
        'created_at',
        'qty',
        'parent_sku',
        'brand',
        'color',
        'cost',
        'flavor',
        'manufacturer',
        'nicotine_level',
        'puff_counts',
        'reg_category',
        'volume',
        'resistance'
    ]
    # Extra columns we want to add.
    extra_columns = ['pack_size', 'canonical_name', 'single_product_sku', 'original_qty', 'upc', 'locations', 'retail_sku']

    # First pass: read all rows and separate parent products from simple products.
    parent_products = {}
    simple_products = []

    with open(input_file, mode='r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            sku = row.get('sku', '').strip()
            product_type = row.get('product_type', '').strip().lower()

            is_in_inventory = False
            vendor_sku_match = None
            
            # Direct match - this sku is a vendor sku in inventory
            if sku in inventory_sku_map:
                vendor_sku_match = sku
                is_in_inventory = True
            else:
                # Try to find if this sku matches any retail_sku in our inventory
                for vendor_sku, data in inventory_sku_map.items():
                    if data['retail_sku'] and data['retail_sku'] == sku:
                        vendor_sku_match = vendor_sku
                        is_in_inventory = True
                        break

            # If this SKU is in the extra CSV, override cost and qty.
            if is_in_inventory and vendor_sku_match:
                inventory_data = inventory_sku_map[vendor_sku_match]
                row['cost'] = inventory_data['cost']
                row['qty'] = inventory_data['qty']
                row['upc'] = inventory_data['upc']
                row['weight'] = inventory_data['weight']
                row['retail_sku'] = sku
                row['sku'] = vendor_sku_match
                row['locations'] = ';'.join(f"{loc}:{qty}" for loc, qty in inventory_data['locations'].items())
            else:
                row['qty'] = 0
                row['locations'] = ''
                row['vendor_sku'] = ''

            # Only add simple products (that are in the extra CSV) to the output.
            if product_type == 'simple':
                simple_products.append(row)
            else:
                # For parent products, we want to load them regardless for fallback values.
                if product_type != 'simple':
                    parent_products[sku] = row

    # Track which inventory items have been processed
    processed_inventory_skus = set()

    # Group products by their canonical key.
    products_by_key = {}
    standalone_products = []
    
    all_rows_to_write = []
    for row in simple_products:
        parent_sku = row.get('parent_sku', '').strip()
        parent = parent_products.get(parent_sku)

        # If fields are missing in the simple product, try to use the parent's values.
        # For numeric fields, we consider empty strings or "0"/"0.0" as missing.
        # For text fields (like flavor), an empty value is considered missing.
        fields_to_check = ['attribute_set_code', 'flavor', 'volume', 'nicotine_level', 'unit_per_pack', 'puff_counts', 'resistance']

        for field in fields_to_check:
            simple_val = row.get(field, '').strip()
            is_missing = (simple_val == '')
            if is_missing and parent:
                parent_val = parent.get(field, '').strip()
                if parent_val:
                    row[field] = parent_val


        # Now process the row using the (potentially updated) values.
        product_name = row.get('name', '')
        unit_per_pack_str = row.get('unit_per_pack', '')
        pack_size = extract_pack_size(product_name, unit_per_pack_str)

        # Process and cast custom fields.
        flavor = row.get('flavor', '').strip()
        nicotine_level = row.get('nicotine_level')
        volume = row.get('volume')
        resistance = row.get('resistance', '').strip()
        category = row.get('attribute_set_code', '').strip()
        
        try:
            puff_count = int(float(row.get('puff_counts', 0)))
        except ValueError:
            puff_count = 0

        # Update row with casted values.
        row['flavor'] = flavor
        row['nicotine_level'] = nicotine_level
        row['volume'] = volume
        row['puff_counts'] = puff_count
        row['resistance'] = resistance
        
        try:
            qty = int(float(row.get('qty', '0')))
        except ValueError:
            qty = 0
        row['original_qty'] = qty  # Store the original quantity
        row['pack_size'] = pack_size

        # Check if this is a inventory item
        is_inventory_item = row['sku'] in inventory_sku_map
        
        # Only generate canonical name if there's enough attribute data
        canonical_name = ""
        if should_generate_canonical_name(volume, nicotine_level, flavor, resistance):
            canonical_name = generate_canonical_name(parent_sku, category, row.get('brand', ''), volume, nicotine_level, flavor, resistance)
            row['canonical_name'] = canonical_name
            
            # Only group products with valid canonical names
            if canonical_name:
                products_by_key.setdefault(canonical_name, []).append(row)
                if is_inventory_item:
                    processed_inventory_skus.add(row['sku'])
                continue

        # For products without a valid canonical name or not grouped
        row['canonical_name'] = canonical_name
        row['single_product_sku'] = ''
        
        # Always keep inventory items
        if is_inventory_item:
            standalone_products.append(row)
            processed_inventory_skus.add(row['sku'])

    # Process the grouped products
    grouped_products = []
    for key, rows in products_by_key.items():
        # the sku of the single pack of the product
        single_product_sku = ''
        for row in rows:
            if int(row['pack_size']) == 1 and row['sku'] in inventory_sku_map:
                single_product_sku = row['sku']
                break
        
        # If no single pack in inventory found, try any single pack
        if not single_product_sku:
            for row in rows:
                if int(row['pack_size']) == 1:
                    single_product_sku = row['sku']
                    break

        # Process each row in the group
        for row in rows:
            # If multi-pack and we have a single pack reference
            if int(row['pack_size']) > 1 and single_product_sku:
                row['single_product_sku'] = single_product_sku
            else:
                row['single_product_sku'] = ''
            
            # Always keep items in inventory
            if row['sku'] in inventory_sku_map:
                grouped_products.append(row)
            # Keep multi-packs that reference an inventory item
            elif row['pack_size'] > 1 and single_product_sku and single_product_sku in inventory_sku_map:
                grouped_products.append(row)

    # Add any inventory items that haven't been processed yet
    for sku, data in inventory_sku_map.items():
        if sku not in processed_inventory_skus:
            # Create a basic row for this inventory item
            row = {
                'sku': sku,
                'name': f"Inventory Item {sku}",  # Basic name
                'cost': data['cost'],
                'price': data['cost'] * 1.5,  # Default markup
                'qty': data['qty'],
                'upc': data['upc'],
                'weight': data['weight'],
                'retail_sku': data['retail_sku'] if data['retail_sku'] else '',
                'pack_size': 1,
                'canonical_name': '',
                'single_product_sku': '',
                'original_qty': data['qty'],
                'locations': ';'.join(f"{loc}:{qty}" for loc, qty in data['locations'].items())
            }
            # Fill in defaults for required columns
            for col in columns_to_keep:
                if col not in row:
                    row[col] = ''
            standalone_products.append(row)
    
    # Combine all products for the final output
    all_rows_to_write = grouped_products + standalone_products

    # Write the final CSV with original columns plus the extra derived columns.
    with open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
        fieldnames = columns_to_keep + extra_columns
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows_to_write:
            output_row = {col: row.get(col, '') for col in fieldnames}
            writer.writerow(output_row)

if __name__ == '__main__':
    magento_csv = 'product_magento_all.csv'                # Update with your CSV file path.
    cleaned_csv = 'magento_cleaned_aggregated.csv'     # Update with desired output path.
    inventory_csv = 'inventory_export.csv'                    # Update with your small CSV containing SKU, Name, Cost, Quantity.
    duoplane_csv = 'dp_product.csv' 

    clean_and_aggregate_magento_csv(magento_csv, cleaned_csv, inventory_csv, duoplane_csv)
    print(f"Aggregated CSV saved to {cleaned_csv}")
