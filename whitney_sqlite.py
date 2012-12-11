import os
import re
import codecs
import sqlite3
import exceptions

data_dir = '/Users/emonson/Programming/ArtMarkets/Whitney'

catalogue_file = 'Whitney_ListOfCatalogues.tsv'
catalogue_path = os.path.join(data_dir, catalogue_file)

data_file = 'Whitney_BookOCR.txt'
data_path = os.path.join(data_dir, data_file)

db_name = 'WhitneyBasic_DB.sqlite'
db_path = os.path.join(data_dir, db_name)

re_name = re.compile(r'^[A-Z]')
re_address = re.compile(r'^([0-9]{4}(?:-(?:1|2|3|II))?) Address: (.+?)$')
re_artwork = re.compile(r'^([0-9]{1,3})\. (.*?)$')

conn = sqlite3.connect(db_path)
c = conn.cursor()

# WARNING: Deleting all tables!!
c.execute("DROP TABLE IF EXISTS catalogues")
c.execute("DROP TABLE IF EXISTS artists")
c.execute("DROP TABLE IF EXISTS addresses")
c.execute("DROP TABLE IF EXISTS show_artist_addresses")
c.execute("DROP TABLE IF EXISTS artworks")
conn.commit()

# Create tables
c.execute('''CREATE TABLE catalogues(
	show_id TEXT PRIMARY KEY,
	show_year INTEGER,
	show_dates_text TEXT,
	show_title TEXT,
	num_pieces INTEGER
)''')

c.execute('''CREATE TABLE artists(
	artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
	name_text TEXT,
	last_name TEXT,
	first_name TEXT,
	year_born INTEGER,
	year_died INTEGER
)''')

c.execute('''CREATE TABLE addresses(
	address_id INTEGER PRIMARY KEY AUTOINCREMENT,
	address_text TEXT
)''')

c.execute('''CREATE TABLE show_artist_addresses(
	show_id TEXT,
	artist_id INTEGER,
	address_id INTEGER,
	PRIMARY KEY (show_id, artist_id),
	FOREIGN KEY (show_id) REFERENCES catalogues(show_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
	FOREIGN KEY (address_id) REFERENCES addresses(address_id)
)''')

c.execute('''CREATE TABLE artworks(
	artwork_id INTEGER PRIMARY KEY AUTOINCREMENT,
	show_id TEXT,
	artist_id INTEGER,
	artwork_text TEXT,
	piece_number INTEGER,
	title TEXT,
	medium TEXT,
	owner TEXT,
	FOREIGN KEY (show_id) REFERENCES catalogues(show_id),
	FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
)''')
conn.commit()

def str_int(s):
	try:
		return int(s)
	except exceptions.ValueError:
		return -1

# Catalogues
print "Loading Catalogues into DB"
for line in codecs.open(catalogue_path, 'r', 'utf-8-sig'):
	fields = line.rstrip().split('\t')
	tag = fields[0]
	year = int(tag[:4])
	date_range = fields[1]
	title = fields[2]
	n_pieces = str_int(fields[3])
	c.execute("INSERT INTO catalogues (show_id, show_year, show_dates_text, show_title, num_pieces) VALUES (?, ?, ?, ?, ?)", (tag, year, date_range, title, n_pieces))

# Artwork Index by Artist
address_ids = {}
print "Loading Data into DB"
for ii,line in enumerate(codecs.open(data_path, 'r', 'utf-8')):
	if ii % 1000 == 0:
		print ii
	
	trimmed_line = line.rstrip()
	name_match = re_name.match(trimmed_line)
	address_match = re_address.match(trimmed_line)
	artwork_match = re_artwork.match(trimmed_line)
	
	if name_match:
		c.execute("INSERT INTO artists (name_text) VALUES (?)", (trimmed_line,))
		artist_id = c.lastrowid
		
	elif address_match:
		show_id = address_match.groups()[0]
		address_text = address_match.groups()[1]
		if address_text not in address_ids:
			c.execute("INSERT INTO addresses (address_text) VALUES (?)", (address_text,))
			address_id = c.lastrowid
			address_ids[address_text] = address_id
		else:
			address_id = address_ids[address_text]
		c.execute("INSERT INTO show_artist_addresses (show_id, artist_id, address_id) VALUES (?,?,?)", (show_id, artist_id, address_id))
		
	elif artwork_match:
		piece_number = artwork_match.groups()[0]
		artwork_text = artwork_match.groups()[1]
		c.execute("INSERT INTO artworks (show_id, artist_id, artwork_text, piece_number) VALUES (?,?,?,?)", (show_id, artist_id, artwork_text, piece_number))
		
	else:
		print "PROBLEM parsing line", ii, trimmed_line
		
conn.commit()
c.close()
