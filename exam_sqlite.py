import os
import re
import codecs
import sqlite3

data_dir = '/Users/emonson/Programming/ArtMarkets/Whitney'

catalogue_file = 'sales_descriptions.txt'
catalogue_path = os.path.join(data_dir, catalogue_file)

data_file = 'sales_contents.txt'
data_path = os.path.join(data_dir, data_file)

db_name = 'exam_DB.sqlite'
db_path = os.path.join(data_dir, db_name)

re_nationality = re.compile(r'\((.+)\)')
re_price = re.compile(r'([0-9]+(\.[0-9]*)?)')

conn = sqlite3.connect(db_path)
c = conn.cursor()

# WARNING: Deleting all tables!!
c.execute("DROP TABLE IF EXISTS catalogues")
c.execute("DROP TABLE IF EXISTS artists")
c.execute("DROP TABLE IF EXISTS artworks")
conn.commit()

# Create tables
c.execute('''CREATE TABLE catalogues (
	catalogue_id TEXT PRIMARY KEY,
	year INTEGER
)''')

c.execute('''CREATE TABLE artists (
	artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT,
	nationality TEXT
)''')

c.execute('''CREATE TABLE artworks (
	artwork_id INTEGER PRIMARY KEY AUTOINCREMENT,
	catalogue_id TEXT,
	artist_id INTEGER,
	title TEXT,
	type TEXT,
	price REAL,
	FOREIGN KEY (catalogue_id) REFERENCES catalogues(catalogue_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
)''')
conn.commit()


# Catalogue descriptions
print "Loading Catalogues into DB"
cat_in = codecs.open(catalogue_path, 'r', 'utf-8')

# Read in titles line
titles = cat_in.readline()

# Keep track of catalogue IDs for debugging
cat_ids = []

for line in cat_in:
	fields = line.rstrip().split('\t')
	tag = fields[0]
	cat_ids.append(tag)
	year = int(fields[1])
	c.execute("INSERT INTO catalogues (catalogue_id, year) VALUES (?, ?)", (tag, year))

# Sales contents
artist_ids = {}
print "Loading Data into DB"
sales_in = codecs.open(data_path, 'r', 'utf-8')

# Read in titles line
titles = sales_in.readline()

for ii,line in enumerate(sales_in):
	if ii % 100 == 0:
		print ii
	
	fields = line.rstrip().split('\t')
	catalogue_id = fields[0]
	# DEBUG
	if catalogue_id not in cat_ids: print 'Catalogue id problem! Line', ii, 'cat', catalogue_id
	name = fields[1]
	title = fields[2]
	obj_type = fields[3]
	transaction = fields[4]

	nationality_match = re_nationality.search(name)
	if nationality_match:
		nationality = nationality_match.groups()[0]
	else:
		# DEBUG
		print 'Nationality problem!!', name
	
	price_match = re_price.search(transaction)

	# Only inserting into DB if has price information
	if price_match:
		price = float(price_match.groups()[0])
		
		# Artists
		if name not in artist_ids:
			c.execute("INSERT INTO artists (name, nationality) VALUES (?,?)", (name,nationality))
			artist_id = c.lastrowid
			artist_ids[name] = artist_id
		else:
			artist_id = artist_ids[name]
		
		# Artworks
		c.execute("INSERT INTO artworks (catalogue_id, artist_id, title, type, price) VALUES (?,?,?,?,?)", (catalogue_id, artist_id, title, obj_type, price))
		artwork_id = c.lastrowid
		
		
conn.commit()
c.close()
