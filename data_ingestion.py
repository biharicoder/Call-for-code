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


class DataIngestor:

	def __init__(self, filename, emission_api):
		self.filename = filename
		self.emission_api = emission_api

	def get_emission_data(self):
		"""Make a get request to get the emission data for the cities of United States
		Parameters: emission_api - url/api to download emissions citywide data
		Returns:
		A dataframe for the emission data
		"""
		response = requests.get(self.emission_api)
		# Print the status code of the response.
		#print(response.status_code)
		emission_data = json.dumps(response.json())
		df_emission_data = pd.read_json(emission_data) 
		print(df_emission_data.head())
		return df_emission_data

	def Clean_names(self):
		"""To remove the state name from city column
		Parameters: City_name - to look upon city column
		Returns: A dataframe where state short form removed from city column
		"""
		k=[]
		df_emission_data=self.get_emission_data()
		for x in df_emission_data.city:
			if re.search(r'\,.*', x): 
				pos = re.search(r'\,.*', x).start()
				k.append(x[:pos])
			else: 
				k.append(x)
		df_emission_data['city']=k
		return df_emission_data


	def get_electricity_consumption(self):
		"""Reads excel file for the electricity consumption
		Parameters: filename - name of the excel file
		Returns:
		A dataframe for the emission data
		"""
		full_path = 'data/'+self.filename
		df_elec = pd.read_excel(full_path, skiprows=3)
		print(df_elec.head())
		return df_elec

	def final_data(self):
		"""Combines the electricity consumption data & emission data
		Parameters: filename - name of the excel file
					emission_api - url/api to download emissions citywide data
		Returns:
		Combined dataframe containing citywide data for emission and power consumption
		"""
		df_emission_data_clean = self.Clean_names()
		df_elec = self.get_electricity_consumption()
		df_final = pd.merge(df_emission_data_clean, df_elec, left_on='city', right_on='Census Division\nand State')
		print(df_final)
		return df_final

if __name__ == '__main__':
	di = DataIngestor('elec_cost.xlsx', "https://data.cdp.net/resource/wii4-buw5.json?country=United States of America")
	df1 = di.get_emission_data()
	df2 = di.get_electricity_consumption()
	df3=di.Clean_names()
	df4=di.final_data()
