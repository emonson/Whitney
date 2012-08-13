import os
import sys
import re
import codecs
import sqlite3
import exceptions
import json
import urllib
import httplib
import time
from pymongo import Connection

# Make a connection to Mongo.
try:
		db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
		print "couldn't connect: be sure that Mongo is running on localhost:27017"
		sys.exit(1)

db = db_conn['whitney']

data_dir = '/Users/emonson/Data/ArtMarkets/Katie'

catalogue_file = 'Whitney_ListOfCatalogues.tsv'
catalogue_path = os.path.join(data_dir, catalogue_file)

data_file = 'Smith_20120801_rev5.txt'
data_path = os.path.join(data_dir, data_file)

db_name = 'Whitney_20120813.sqlite'
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
c.execute("DROP TABLE IF EXISTS yahoo_geocodes")
c.execute("DROP TABLE IF EXISTS show_artist_addresses")
c.execute("DROP TABLE IF EXISTS artworks")

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

c.execute(''' CREATE TABLE yahoo_geocodes(
	geocode_id INTEGER PRIMARY KEY AUTOINCREMENT,
	address_id INTEGER,
	multiples_rank INTEGER,
	quality INTEGER,
	latitude REAL,
	longitude REAL,
	radius REAL,
	city TEXT,
	state TEXT,
	country TEXT,
	FOREIGN KEY (address_id) REFERENCES addresses(address_id)
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

def str_int(s):
	try:
		return int(s)
	except exceptions.ValueError:
		return -1

def geocode_address(address_txt):
	# Yahoo Geocode service
	http_host = 'where.yahooapis.com'
	headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_4) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.75 Safari/537.1', 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3'}
	http_conn = httplib.HTTPConnection(http_host)
	query_dict = {'appid': 'cp7E7D7i', 'flags': 'J', 'locale':'en_US'}
	
	# Take care of some special cases
	if address_txt.startswith('c/o') and (',' in address_txt):
		txt = address_txt[address_txt.find(',')+1:]
	else:
		txt = address_txt

	query_dict['q'] = txt.encode('utf-8')
	query_str = urllib.urlencode(query_dict, True)
	# print query_str
	
	search_url = '/geocode?' + query_str
	# print search_url
	# sys.stdout.flush()
		
	http_conn.request("GET", search_url, None, headers)
	
	resp = http_conn.getresponse()
	# print "Response Status:", resp.status
	# sys.stdout.flush()
	
	if resp.status == 200:
		returned_json = resp.read()
		http_conn.close()
		return returned_json
	else:
		# Try once more...
		time.sleep(0.25)
		http_conn.request("GET", search_url, None, headers)
		time.sleep(0.25)
		resp = http_conn.getresponse()
		
		if resp.status == 200:
			print '* * Returning second try response'
			returned_json = resp.read()
			http_conn.close()
			return resp.read()
	
	returned_status = resp.status
	http_conn.close()
	return returned_status
	
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

			# DEBUG
			print 'address:', address_id, '-' + address_text + '-'
			# Geocode
			
			# First check if the item info is already in our own database
			geo_dict = db.yahoo_geo.find_one({'address_text':address_text})

			# Only hit Yahoo if address is not in MongoDB
			if geo_dict is None:
				geo_resultset_json = geocode_address(address_text)
				# NOTE: This will just error out if yahoo returns nothing...
				try:
					geo_dict = json.loads(geo_resultset_json)
				except:
					print 'Response:', geo_resultset_json
					sys.exit(1)
				
				geo_dict['address_text'] = address_text
				# Save it in MongoDB
				db.yahoo_geo.save(geo_dict)
			
			geo_resultset_dict = geo_dict['ResultSet']
			if geo_resultset_dict['Found'] > 0:
				# Keeping track of the order of results since Yahoo seems to return the best first
				for rr, gr in enumerate(geo_resultset_dict['Results']):
					multiples_rank = rr
					quality = None
					latitude = None
					longitude = None
					radius = None
					city = None
					state = None
					country = None
					if 'quality' in gr: quality = gr['quality']
					if 'latitude' in gr: latitude = gr['latitude']
					if 'longitude' in gr: longitude = gr['longitude']
					if 'radius' in gr: radius = gr['radius']
					if 'city' in gr: city = gr['city']
					if 'state' in gr: state = gr['state']
					if 'country' in gr: country = gr['country']
					c.execute("INSERT INTO yahoo_geocodes (	address_id, quality, multiples_rank, latitude, longitude, radius, city, state, country) VALUES (?,?,?,?,?,?,?,?,?)", (address_id, quality, multiples_rank, longitude, latitude, radius, city, state, country))
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
