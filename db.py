import sqlite3
import asyncio
from fetch_utility import now

def db_initialization(path: str):
    """Initializes DB connection, cursor, and sets up the corresponding tables."""
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
    cur = conn.cursor()
    db_lock = asyncio.Lock()
        
    db ={
        "conn": conn,
        "cur": cur,
        "lock": db_lock,
    }
    
# Create tables if not exist
    db["cur"].executescript('''
        CREATE TABLE IF NOT EXISTS Urls (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            date TEXT,
            content_hash TEXT,
            changer INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS Links (
            from_id INTEGER,
            to_id INTEGER,
            PRIMARY KEY (from_id, to_id)
        );
        
        CREATE TABLE IF NOT EXISTS Category (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url_id INTEGER,
            name TEXT,
            UNIQUE(name, url_id)                 
        );
                        
        CREATE TABLE IF NOT EXISTS PageKeywords (
            url_id INTEGER,
            keyword TEXT,
            count INTEGER,
            PRIMARY KEY (url_id, keyword)
        );
                        
        CREATE TABLE IF NOT EXISTS Products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url_id INTEGER,
        title TEXT,
        price REAL,
        stock INTEGER,
        rating TEXT,
        image_url TEXT,
        UNIQUE(title, url_id) 
        );                    
    ''')

    db["conn"].commit()

    return db

def insert_url_and_get_id(normalized_link, db):
    """Insert a URL if it doesn't exist, and return its ID whether it's new or already in the database."""
    db["cur"].execute('SELECT date, id FROM Urls WHERE name=?', (normalized_link,))
    row = db["cur"].fetchone()
    if row is None:
        db["cur"].execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (normalized_link,))
        db["cur"].execute('SELECT id FROM Urls WHERE name=?', (normalized_link,))
        new_row = db["cur"].fetchone()
        id_of_url = new_row[0] if new_row else None
    else:
        id_of_url = row[1]  # id of existing URL

    return id_of_url

def insert_link_relationship(normalized_link, db, from_id):
    """Insert link relationship into the database."""
    if from_id is not None:
        db["cur"].execute('SELECT id FROM Urls WHERE name=?', (normalized_link,))
        to_row = db["cur"].fetchone()
        if to_row:
            to_id = to_row[0]
            db["cur"].execute(
                'INSERT OR IGNORE INTO Links (from_id, to_id) VALUES (?, ?)',
                (from_id, to_id)
            )

def insert_category(db, category, from_id):
    """Insert category into databse."""
    # --- Save category and keywords ---
    if category:
        db["cur"].execute(
            'INSERT OR IGNORE INTO Category (url_id, name) VALUES (?, ?)',
            (from_id, category)
        )

def insert_keywords(db, keywords, from_id):
    """Store extracted keywords for a given URL in the database.
    """
    for keyword, count in keywords:
        db["cur"].execute(
            'INSERT OR REPLACE INTO PageKeywords (url_id, keyword, count) VALUES (?, ?, ?)',
            (from_id, keyword, count)
        )

def insert_product(from_id, db, product_data):
    """Store product information related to a URL in the database.
    """
    if product_data and from_id:
        
        db["cur"].execute('''
        INSERT OR IGNORE INTO Products (url_id, title, price, stock, rating, image_url)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (from_id, product_data["title"], product_data["price"], product_data["stock"], product_data["rating"], product_data["image_url"])
        )


def get_url_id(db, normalized_link):
    """Retrieve the database ID for a given URL if it exists.
    """
    db["cur"].execute('SELECT date, id FROM Urls WHERE name=?', (normalized_link,))
    row = db["cur"].fetchone()
    if row is None:
        db["cur"].execute('INSERT OR IGNORE INTO Urls (name) VALUES (?)', (normalized_link,))


async def save_to_db(ctx, currenturl, to_enqueue, link_pairs, product_data, category, keywords):
    """
    Perform batch insertion of a page’s URL, links, product data, category, and keywords
    into the database in an atomic, non-blocking manner.
    """

    # DB writes in a single lock
    async with ctx.db["lock"]:
        # get from_id
        ctx.db["cur"].execute('SELECT id FROM Urls WHERE name=?', (currenturl,))
        row = ctx.db["cur"].fetchone()
        from_id = row[0] if row else None

        # Insert all URLs (idempotent)
        inserted_ids = {}
        for normalized_link, _ in to_enqueue:
            new_id = insert_url_and_get_id(normalized_link, ctx.db)
            inserted_ids[normalized_link] = new_id

        # Insert link relationships (only if from_id exists)
        if from_id is not None:
            for normalized_link in link_pairs:
                insert_link_relationship(normalized_link, ctx.db, from_id)

        if product_data and from_id:
            try:
                # insert product and commit                       
                insert_product(from_id, ctx.db, product_data)
                insert_category(ctx.db, category, from_id)
                insert_keywords(ctx.db, keywords, from_id)
                ctx.logger.info(f"Product/category/keywords saving succesfull for {currenturl}")
                #Stamp date in db           
                ctx.db["cur"].execute('UPDATE Urls SET date = ? WHERE name = ?', (now(), currenturl))
                
            except Exception as e:

                ctx.error_logger.error(f"Product/category/keywords saving failed for {currenturl}: {e}", exc_info=True)

        ctx.db["conn"].commit()

    return inserted_ids