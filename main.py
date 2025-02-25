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

def generate_canonical_name(base_name, volume, nicotine_level, flavor):
    """
    Build a canonical name using the provided base name and additional attributes.
    Only non-empty attributes are added, separated by "|".
    """
    base_name = base_name.strip().lower()
    attrs = []
    for attr in [str(volume), str(nicotine_level), flavor]:
        attr_str = attr.strip().lower()
        if attr_str and attr_str not in ['0', '0.0']:
            attrs.append(attr_str)
    canonical = base_name
    if attrs:
        canonical += " | " + " | ".join(attrs)
    return canonical

def load_cost_quantity_data(extra_csv_file):
    """
    Load the extra CSV that contains the correct Cost and Quantity,
    and return a dictionary keyed by SKU.
    """
    cost_qty_map = {}
    with open(extra_csv_file, mode='r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            sku = row.get('SKU', '').strip()
            try:
                cost = float(row.get('Cost', '0').strip())
            except ValueError:
                cost = 0.0
            try:
                quantity = int(float(row.get('Quantity', '0').strip()))
            except ValueError:
                quantity = 0

            if quantity < 0:
                quantity = 0
            if sku:
                cost_qty_map[sku] = {'cost': cost, 'qty': quantity}

    return cost_qty_map

def clean_and_aggregate_magento_csv(input_file, output_file, extra_csv_file):
    # Load the extra CSV with correct cost and quantity.
    cost_qty_map = load_cost_quantity_data(extra_csv_file)

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
        'volume'
    ]
    # Extra columns we want to add.
    extra_columns = ['pack_size', 'canonical_name', 'single_product_sku', 'original_qty']

    # First pass: read all rows and separate parent products from simple products.
    parent_products = {}
    simple_products = []

    with open(input_file, mode='r', encoding='utf-8', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            sku = row.get('sku', '').strip()
            product_type = row.get('product_type', '').strip().lower()

            # If this SKU is in the extra CSV, override cost and qty.
            if sku in cost_qty_map:
                row['cost'] = cost_qty_map[sku]['cost']
                row['qty'] = cost_qty_map[sku]['qty']
                # Only add simple products (that are in the extra CSV) to the output.
                if product_type == 'simple':
                    simple_products.append(row)
            else:
                # For parent products, we want to load them regardless for fallback values.
                if product_type != 'simple':
                    parent_products[sku] = row

    # Group products by their canonical key.
    products_by_key = {}
    
    for row in simple_products:
        parent_sku = row.get('parent_sku', '').strip()
        parent = parent_products.get(parent_sku)

        # If fields are missing in the simple product, try to use the parent's values.
        # For numeric fields, we consider empty strings or "0"/"0.0" as missing.
        # For text fields (like flavor), an empty value is considered missing.
        fields_to_check = ['flavor', 'volume', 'nicotine_level', 'unit_per_pack', 'puff_counts']
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

        try:
            puff_count = int(float(row.get('puff_counts', 0)))
        except ValueError:
            puff_count = 0

        # Update row with casted values.
        row['flavor'] = flavor
        row['nicotine_level'] = nicotine_level
        row['volume'] = volume
        row['puff_counts'] = puff_count

        # Generate the canonical name using the parent's SKU (or product name) plus attributes.
        canonical_name = generate_canonical_name(parent_sku, volume, nicotine_level, flavor)
        
        try:
            qty = int(float(row.get('qty', '0')))
        except ValueError:
            qty = 0
        row['original_qty'] = qty  # Store the original quantity
        row['pack_size'] = pack_size
        row['canonical_name'] = canonical_name

        # Group by the canonical name.
        products_by_key.setdefault(canonical_name, []).append(row)

    # Aggregate inventory for each group:
    # Multiply each row's qty by its pack_size to get the total singles.
    # Choose one representative row (preferably one with pack_size == 1) to hold the aggregated total.
    all_rows_to_write = []
    for key, rows in products_by_key.items():
        total_singles = 0
        rep_index = None
        # the sku of the single pack of the product
        single_product_sku = ''
        
        for i, row in enumerate(rows):
            # ... existing qty_val and total_singles calculation ...

            if int(row['pack_size']) == 1:
                if rep_index is None:
                    rep_index = i
                if not single_product_sku:  # Capture first single pack SKU
                    single_product_sku = row['sku']

        for i, row in enumerate(rows):
            try:
                qty_val = int(float(row.get('qty', 0)))
            except ValueError:
                qty_val = 0
            total_singles += int(row['pack_size']) * qty_val
            if int(row['pack_size']) == 1 and rep_index is None:
                rep_index = i

        if rep_index is None:
            rep_index = 0

        for i, row in enumerate(rows):
            if i == rep_index:
                row['qty'] = total_singles  # aggregated total
            else:
                # TODO: product without single pack.
                row['qty'] = 0  # zero out other rows in the group
            row['single_product_sku'] = single_product_sku   # Add to all rows in group
            all_rows_to_write.append(row)

    # Write the final CSV with original columns plus the extra derived columns.
    with open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
        fieldnames = columns_to_keep + extra_columns
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows_to_write:
            output_row = {col: row.get(col, '') for col in fieldnames}
            writer.writerow(output_row)

if __name__ == '__main__':
    magento_csv = 'magento_export.csv'                # Update with your CSV file path.
    cleaned_csv = 'magento_cleaned_aggregated.csv'     # Update with desired output path.
    extra_csv = 'extra_cost_qty.csv'                    # Update with your small CSV containing SKU, Name, Cost, Quantity.
    clean_and_aggregate_magento_csv(magento_csv, cleaned_csv, extra_csv)
    print(f"Aggregated CSV saved to {cleaned_csv}")
