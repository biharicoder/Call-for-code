# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 5 16:55:33 2020
@author: shivamsolanki
"""
import json
import requests
import pandas as pd
import re
from geopy.geocoders import Nominatim


class DataIngestor:

	def __init__(self, filename, emission_api):
		self.filename = filename
		self.emission_api = emission_api

	def get_emission_data(self):
		"""Make a get request to get the emission data for the cities of United States
		Parameters: emission_api - url/api to download emissions citywide data
		Returns:
		A dataFrame for the emission data
		"""
		response = requests.get(self.emission_api)
		# Print the status code of the response.
		print(response.status_code)

		emission_data = json.dumps(response.json())

		df_emission_data = pd.read_json(emission_data)
		return df_emission_data

	def get_state_name(self):
		state_list = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
                    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
	                "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
	                "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
		            "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
		            "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
		            "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
		            "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]

		geolocator = Nominatim(user_agent="solar_app")

		df = self.get_emission_data()
		# Extracting coordinate from geocoded_column & reversing list to get the correct order of lat and long
		df['coordinates'] = df['geocoded_column'].apply(lambda x: x['coordinates'][::-1])
		df['location'] = df['coordinates'].apply(lambda x: geolocator.reverse(x))
		df['address'] = df['location'].apply(lambda x: x.address)
		# Dropping rows with invalid lat, long values
		df = df.dropna(subset=['address'])
		df['addr_list'] = df['address'].apply(lambda x: str.split(x, ','))
		df['addr_list'] = df['addr_list'].apply(lambda x: [i.lstrip() for i in x])
		df['state'] = df['addr_list'].apply(lambda x: [i for i in x if i in state_list])
		df['state'] = df['state'].apply(lambda x: ", ".join(x))
		# Dropping rows with invalid lat, long values
		df = df.dropna(subset=['state'])
		print(df[['coordinates', 'location', 'address', 'addr_list', 'state']])
		return df

	@staticmethod
	def get_city_name(column_name):
		"""Removes the state name from city column
		Parameters: column_name - name of the column in which cleaning is required
		Returns: cleaned column
		"""
		if re.search(r'\,.*', column_name):
			pos = re.search(r'\,.*', column_name).start()
			city = column_name[:pos]
			return city
		else:
			return column_name

	def get_electricity_consumption(self):
		"""Reads excel file for the electricity consumption
		Parameters: filename - name of the excel file
		Returns:
		A dataFrame for the emission data
		"""
		full_path = 'data/'+self.filename
		df_elec = pd.read_excel(full_path, skiprows=3)
		print(df_elec.head())
		return df_elec

	def final_data(self):
		"""Combines the electricity consumption data & emission data
		Returns:
		Combined dataFrame containing citywide data for emission and power consumption
		"""
		df_emission_data = self.get_state_name()
		df_elec = self.get_electricity_consumption()
		df_final = pd.merge(df_emission_data, df_elec, left_on='state', right_on='Census Division\nand State')
		print(df_final.head())
		return df_final


if __name__ == '__main__':
	di = DataIngestor('elec_cost.xlsx', "https://data.cdp.net/resource/wii4-buw5.json?country=United States of America")
	df = di.final_data()
