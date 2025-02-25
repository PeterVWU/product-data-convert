import pandas as pd

# Input CSV from Magento
input_filename = "magento_cleaned_aggregated.csv"
df = pd.read_csv(input_filename)

odoo_df = pd.DataFrame({
    "default_code": df["sku"],
    "name": df["canonical_name"],
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
odoo_inventory_qty = pd.DataFrame({
    "product": df["sku"],
    "inventoried_quantity": df["qty"],
})

#TODO: BoM import file

# Save the new DataFrame to a CSV file for Odoo import.
output_filename = "odoo_import.csv"
odoo_df.to_csv(output_filename, index=False)

# Save the new DataFrame to a CSV file for Odoo import.
odoo_inventory_qty_filename = "odoo_inventory_qty_import.csv"
odoo_inventory_qty.to_csv(odoo_inventory_qty_filename, index=False)

print(f"CSV for Odoo import has been created: {output_filename}")
