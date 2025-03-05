import pandas as pd

# Load the Odoo export CSV (contains ID and default_code/SKU)
odoo_export_filename = "odoo_products_export.csv"
odoo_df = pd.read_csv(odoo_export_filename)

# Load the update data (contains SKU and new barcode values)
updates_filename = "odoo_import.csv"
updates_df = pd.read_csv(updates_filename)

# Convert barcode column to string and remove decimals
updates_df['barcode'] = updates_df['barcode'].fillna('').astype(str).apply(lambda x: x.split('.')[0] if '.' in x else x)

# Create a mapping from SKU to barcode, excluding empty barcodes
sku_to_barcode = {
    sku: barcode 
    for sku, barcode in zip(updates_df['default_code'], updates_df['barcode']) 
    if barcode and barcode != ''
}

# Create the update DataFrame
update_rows = []
for _, row in odoo_df.iterrows():
    sku = row['default_code']
    if sku in sku_to_barcode and pd.notna(sku_to_barcode[sku]):
        update_rows.append({
            'id': row['id'],
            'barcode': sku_to_barcode[sku]
        })

# Convert to DataFrame and save
if update_rows:
    updates_output = pd.DataFrame(update_rows)
    output_filename = "odoo_product_updates.csv"
    updates_output.to_csv(output_filename, index=False)
    print(f"Created update file: {output_filename}")
    print(f"Number of products to update: {len(update_rows)}")
else:
    print("No updates found")