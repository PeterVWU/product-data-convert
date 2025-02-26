import pandas as pd

# Input CSV from Magento
input_filename = "magento_cleaned_aggregated.csv"
df = pd.read_csv(input_filename)

odoo_df = pd.DataFrame({
    "default_code": df["sku"],
    "name": df["name"],
    "categ_id": df["attribute_set_code"],
    "list_price": df["price"],
    "standard_price": df["cost"],
    "type": "goods", 
    "x_puff_count": df["puff_counts"],
    "x_flavor": df["flavor"],
    "x_volume": df["volume"],
    "x_nicotine_level": df["nicotine_level"],
    "x_pack_size": df["pack_size"],
})

#TODO: BoM import file
valid_components = df["sku"].unique()
filtered_df = df[(df['pack_size'] != 1) & 
                (df['single_product_sku'].notna()) & 
                (df['single_product_sku'].str.strip() != '') &
                (df['single_product_sku'].isin(valid_components))]
odoo_bom = pd.DataFrame({
    "product": filtered_df['sku'],
    "BoM Type": "Kit",
    "BoM Lines/Component": filtered_df['single_product_sku'],
    "BoM Lines/Quantity": filtered_df['pack_size']
})


# Create mask for products that are NOT in the BoM
non_bom_mask = ~df['sku'].isin(filtered_df['sku'])
odoo_inventory_qty = pd.DataFrame({
    "product": df[non_bom_mask]["sku"],
    "inventoried_quantity": df[non_bom_mask]["qty"],
})


# Save the new DataFrame to a CSV file for Odoo import.
output_filename = "odoo_import.csv"
odoo_df.to_csv(output_filename, index=False)

# Save the new DataFrame to a CSV file for inventory quantity import.
odoo_inventory_qty_filename = "odoo_inventory_qty_import.csv"
odoo_inventory_qty.to_csv(odoo_inventory_qty_filename, index=False)

# Save the new DataFrame to a CSV file for Bill of Material import.
odoo_bom_filename = "odoo_bom_import.csv"
odoo_bom.to_csv(odoo_bom_filename, index=False)

print(f"CSV for Odoo import has been created: {output_filename}")
