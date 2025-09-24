import sqlite3
import json
import logging
import csv
import sys


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_products_to_csv(db_path='mini.sqlite', output_file='crawl_export.csv'):
    """
    Export products from SQLite database to a CSV file.
    Includes URL and categories.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fetch products with URL and categories
    query = '''
    SELECT 
        p.id,
        u.name as url,
        p.title,
        p.price,
        p.stock,
        p.rating,
        p.image_url,
        GROUP_CONCAT(c.name) as categories
    FROM Products p
    JOIN Urls u ON p.url_id = u.id
    LEFT JOIN Category c ON u.id = c.url_id
    GROUP BY p.id
    '''
    cur.execute(query)

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Header
        writer.writerow(['url', 'title', 'price', 'stock', 'rating', 'image_url', 'categories'])

        # Write rows
        for row in cur.fetchall():
            prod_id, url, title, price, stock, rating, image_url, categories = row
            writer.writerow([url, title, price, stock, rating, image_url, categories if categories else ''])

    logging.info(f"Exported products to {output_file}")
    conn.close()


##############################################

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_products_to_json(db_path='mini.sqlite', output_file='crawl_export.json'):
    """
    Export products from SQLite database to a JSON file.
    Includes URL and categories.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fetch products with URL and categories
    query = '''
    SELECT 
        p.id,
        u.name as url,
        p.title,
        p.price,
        p.stock,
        p.rating,
        p.image_url,
        GROUP_CONCAT(c.name) as categories
    FROM Products p
    JOIN Urls u ON p.url_id = u.id
    LEFT JOIN Category c ON u.id = c.url_id
    GROUP BY p.id
    '''
    cur.execute(query)

    products = []
    for row in cur.fetchall():
        prod_id, url, title, price, stock, rating, image_url, categories = row
        product_data = {
            "url": url,
            "title": title,
            "price": price,
            "stock": stock,
            "rating": rating,
            "image_url": image_url,
            "categories": categories.split(",") if categories else []
        }
        products.append(product_data)

    # Write JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(products, f, indent=4, ensure_ascii=False)

    logging.info(f"Exported {len(products)} products to {output_file}")
    conn.close()

#########################################

# Prompt user
sval = input("Enter 1 for JSON, 2 for CSV or 0 to exit: ")

if sval == "1":
    export_products_to_json()
elif sval == "2":
    export_products_to_csv()
elif sval == "0":
    sys.exit()
else:
    print("Invalid choice.")
