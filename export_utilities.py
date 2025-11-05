
QUERY_EXPORT = '''
SELECT Urls.id, Urls.name, Urls.date, Category.name, Products.title, Products.rating, Products.price, Products.stock
FROM Urls
LEFT JOIN Category ON Urls.id = Category.url_id
LEFT JOIN Products ON Urls.id = Products.url_id
ORDER BY Urls.id;
'''

#Def export JSON
def export_to_json(db, filename='exported_data.json'):

    """Export crawling results stored in databse to a json file."""

    import json

    rows = db["cur"].execute(QUERY_EXPORT)

    list_of_url_objects = list()

    for row in rows:

        url_id = row[0]
        url_title = row[1]
        
        if (row[4] == None):
            continue
        else:
            category = row[3]
            product_title = row[4]
            product_rating = row[5]
            product_price = row[6]
            product_stock = row[7]

        temporary_dic_object = {}

        temporary_dic_object["id"] = url_id
        temporary_dic_object["url_title"] = url_title
        temporary_dic_object["category"] = category
        temporary_dic_object["product_title"] = product_title
        temporary_dic_object["product_rating"] = product_rating
        temporary_dic_object["product_price"] = product_price
        temporary_dic_object["product_stock"] = product_stock

        list_of_url_objects.append(temporary_dic_object)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(list_of_url_objects, f, indent=4, ensure_ascii=False)

    print(f"Exported crawl results to exported_data")

#Def export CSV
def export_to_csv(db, filename='exported_data.csv'):

    """Export crawling results stored in databse to a csv file."""

    import csv
        
    rows = db["cur"].execute(QUERY_EXPORT)

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        #Headers
        writer.writerow([
            'id', 'url_title', 'date', 'category',
            'product_title', 'product_rating', 'product_price', 'product_stock'
        ])

        # Write rows
        for row in rows:
            if row[4] is None:
                continue
            else:
                writer.writerow(row)

    print(f"Exported crawl results to {filename}")