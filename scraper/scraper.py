# ols-execute.py version 2022.7.5

# Dependencies:
# pip install beautifulsoup4 requests

# Example usage:
# python ols-execute.py

import csv
import datetime
import time
import json
import re
import requests
import string
import shutil
import os.path
import urllib.parse
import urllib.request
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from collections import OrderedDict
from itertools import zip_longest
from typing import Dict
from time import sleep

class Scraper :

	def getCsvTo2DArray(this,filename, include_header = True) :
		fin = open(filename, 'r');
		csv_reader = csv.reader(fin)
		rows = []
		for row in csv_reader:
			if not len(row):
				continue;
			rows.append(row)
		if(include_header is False):
			rows.pop(0)
		return rows;

	def writeToCsv(this,filename, fields, mode='a') :
		if isinstance(fields, OrderedDict):
			fields = dict(fields).values();
		if isinstance(fields, Dict):
			fields = fields.values();
		out = open(filename, mode, newline='');
		writer = csv.writer(out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL);
		writer.writerow(fields);
		out.close();

	def writeToTopOfCsv(this,filename, fields, mode='a') :
		if isinstance(fields, OrderedDict):
			fields = dict(fields).values();
		if isinstance(fields, Dict):
			fields = fields.values();
		with open(filename, "r") as infile:
			reader = list(csv.reader(infile));
			reader.insert(1, fields);
		with open(filename, "w", newline='') as outfile:
			writer = csv.writer(outfile);
			for line in reader:
				writer.writerow(line);

	def send(this,method = 'get', url = None, data = None) :
		if(not hasattr(this, "session")):
			this.session = requests.Session();
			this.session.mount('http://', HTTPAdapter(max_retries=5))
		response = this.session.request(method, url=url, data=data, timeout=20);
		if(not response.content):
			response = this.session.request(method, url=url, data=data, timeout=20);
		sleep(0.10);
		response.contents = response.content.decode('ISO-8859-1');
		return response;

	def sendSafely(this,method = 'get', url = None, data = None) :
		response = this.send(method, url, data);
		if("encountered an unexpected problem" in response.contents
			or "servlet/WelcomeController" in response.contents
			or "Service Temporarily Unavailable" in response.contents):
			this.executeEntrance();
			sleep(2);
			response = this.sendSafely(method, url, data);
		return response;

	def executeEntrance(this) :
		url = 'http://www.oregonliquorsearch.com/servlet/WelcomeController';
		data = {"btnSubmit": "I'm 21 or older"};
		contents = this.send('post', url, data).contents;

	def executeLocations(this) :
		url = 'http://www.oregonliquorsearch.com/browse_locations.jsp';
		contents = this.sendSafely('get', url).contents;
		matches = re.findall(r'&city=([^/"\']+)', contents);
		for match in matches : 
			this.executeLocation(match);
	
	def executeLocation(this,location = '', page = None) :
		location_encoded = urllib.parse.quote(location);
		url = "http://www.oregonliquorsearch.com/servlet/FrontController?view=browselocations&action=select&city=" + str(location_encoded);
		print(f"\nlocation: {location}", end='');
		contents = this.sendSafely('get', url).contents;
		matches = re.findall(r'class="link">([^/"\']+)<', contents);
		if(not matches):
			this.executeEntrance();
			contents = this.sendSafely('get', url).contents;
			matches = re.findall(r'class="link">([^/"\']+)<', contents);
		if(not matches):
			print(f"\nCould not retreive {location}");
			exit();
		for match in matches : 
			this.executeStore(location, match);
	
	def executeStore(this,location = '', store_id = '') :
		print(f"\nagencyNumber: {store_id}", end='');
		print(f"\npage:", end='');
		url = "http://www.oregonliquorsearch.com/servlet/FrontController?view=browsesublocations&action=select&agencyNumber="+store_id+"&locationRowNum=1";
		contents = this.sendSafely('get', url).contents;
		url = "http://www.oregonliquorsearch.com/servlet/FrontController?view=locationdetails&agencyNumber="+store_id+"&action=pagechange&locationRowNum=1&column=Description&pageSize=100&pageCurrent=1";
		contents = this.sendSafely('get', url).contents;
		if(not store_id in this.stores):
			dom = BeautifulSoup(contents, 'html.parser');
			store_addr = dom.select_one('#location-display p:nth-child(2)').text;
			store = re.split(r"[\t\r\n]+", store_addr);
			store[1] = store[1].strip(',');
			store_phone = dom.select_one('#location-display p:nth-child(3)').text;
			store.append(store_phone);
			this.stores[store_id] = store;
		last_page = 1;
		matches = re.findall(r'pageCurrent=([0-9]+)', contents);
		if(len(matches) > 1):
			last_page = matches[-2];
		for page in range(1, int(last_page) + 1):
			this.executeStorePage(location, store_id, page);
	
	def executeStorePage(this,location = '', store_id = '', page = 1) :
		url = "http://www.oregonliquorsearch.com/servlet/FrontController?view=locationdetails&agencyNumber="+store_id+"&action=pagechange&locationRowNum=1&column=Description&pageSize=100&pageCurrent="+str(page);
		print(f" {page}", end='', flush=True);
		contents = this.sendSafely('get', url).contents;
		dom = BeautifulSoup(contents, 'html.parser');
		rows = dom.select('table.list tr.alt-row, table.list tr.row');
		for row in rows:
			fields = [];
			tds = row.select('td');
			for td in tds:
				fields.append(td.text.strip());
			item_id = fields[0]+fields[1];
			if(not item_id in this.items):
				url = f"http://www.oregonliquorsearch.com/servlet/FrontController?view=locationdetails&agencyNumber={store_id}&action=productselect&itemCode={fields[1]}&newItemCode={fields[0]}&locationRowNum=1&productRowNum=1";
				contents = this.sendSafely('get', url).contents;
				matches = re.findall(r'>Category:<.+?\n.+?>(.+?)<', contents);
				category = '';
				if(matches):
					category = matches[0];
				item = [category];
				this.items[item_id] = item;
			fields[:0] = this.items[item_id];
			fields[:0] = this.stores[store_id];
			fields[:0] = [store_id];
			uniquekey = store_id+item_id;
			this.unique[uniquekey] = 1;
			this.writeToCsv(this.out_fn, fields, 'a');
	
	def execute(this,only_new = True) :
		this.unique = {};
		this.stores = {};
		this.items = {};
		this.date = datetime.date.today().strftime('%Y%m%d');
		this.timestamp = int(time.time());
		this.out_fn = "ols-results-latest.csv";
		this.bk_fn = f"ols-results-{this.date}-{this.timestamp}.csv";
		if (os.path.isfile(this.out_fn)) :
			shutil.copyfile(this.out_fn, this.bk_fn);
		fields = [];
		fields.append('Store ID');
		fields.append('Address');
		fields.append('City');
		fields.append('State');
		fields.append('Postcode');
		fields.append('Phone #');
		fields.append('Category');
		fields.append('New Item Code');
		fields.append('Item Code');
		fields.append('Description');
		fields.append('Size');
		fields.append('Proof');
		fields.append('Age');
		fields.append('Case Price');
		fields.append('Bottle Price');
		fields.append('Qty');
		this.writeToCsv(this.out_fn, fields, 'w');
		this.executeLocations();

# scraper = Scraper();
# scraper.execute();