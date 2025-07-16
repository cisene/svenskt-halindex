#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import datetime

import sqlite3
from sqlite3 import Error

import yaml

APP_NAME = 'SHI-DatabaseAddLayer'
APP_VERSION = '0.0.1'

COORDINATES_SCHEMA = {
  'longitude': {
    'columnName': 'longitude',
    'columnType': 'real'
  },
  'latitude': {
    'columnName': 'latitude',
    'columnType': 'real'
  },
  'layertype': {
    'columnName': 'layertype',
    'columnType': 'text'
  },
  'urn': {
    'columnName': 'urn',
    'columnType': 'text'
  },
  'country': {
    'columnName': 'country',
    'columnType': 'text'
  },
  'city': {
    'columnName': 'city',
    'columnType': 'text'
  },
  'location': {
    'columnName': 'location',
    'columnType': 'text'
  }
}

def loadYAML(filepath):
  result = None
  file_contents = None
  if os.path.isfile(filepath):
    fp = None
    try:
      fp = open(filepath)
      file_contents = fp.read()
      fp.close()

    finally:
      pass

  if file_contents != None:
    result = yaml.safe_load(file_contents)

  return result

def destroy_connection(conn):
  if conn:
    conn.close()
    conn = None
  return

def create_connection(database_file):
  conn = None
  try:
    conn = sqlite3.connect(database_file)
  except Error as e:
    print(e)
  finally:
    pass

  return conn

def processContents(contents, connection):
  query_buffer = []
  cursor = connection.cursor()

  loc_layertype = contents['meta']['category']

  for loc in contents['locations']:

    loc_location = None
    loc_urn = None
    loc_city = None
    loc_country = None
    loc_latitude = None
    loc_longitude = None

    if "location" in loc:
      loc_location = str(loc['location'])
    else:
      loc_location = ''

    if "urn" in loc:
      loc_urn = str(loc['urn'])
    else:
      loc_urn = ''

    if "city" in loc:
      loc_city = str(loc['city'])
    else:
      loc_city = ''

    if "country" in loc:
      loc_country = str(loc['country'])
    else:
      loc_country = ''

    if "latitude" in loc:
      loc_latitude = float(loc['latitude'])
    else:
      continue

    if "longitude" in loc:
      loc_longitude = float(loc['longitude'])
    else:
      continue

    values = f"({loc_longitude}, {loc_latitude}, '{loc_layertype}', '{loc_urn}', '{loc_country}', '{loc_city}', '{loc_location}')"
    #print(values)
    query_buffer.append(values)

    if len(query_buffer) >= 500:
      query_values = ",".join(query_buffer)

      query_buffer = []
      query = f"INSERT INTO locations (longitude, latitude, layertype, urn, country, city, location) VALUES {query_values} ;"

      #print(query)
      try:
        cursor.execute(query)
        connection.commit()

      except sqlite3.IntegrityError as e:
        #print(e)
        pass

      finally:
        pass


  if len(query_buffer) > 0:
    query_values = ",".join(query_buffer)

    query_buffer = []
    query = f"INSERT INTO locations (longitude, latitude, layertype, urn, country, city, location) VALUES {query_values} ;"

    #print(query)
    try:
      cursor.execute(query)
      connection.commit()

    except sqlite3.IntegrityError as e:
      #print(e)
      pass

    except:
      pass

    finally:
      pass

def main():
  #print(f"{APP_NAME} {APP_VERSION}")

  database_path = './database.sqlite'

  connection = create_connection(database_path)
  if connection != None:
    pass

  #print(sys.argv)

  arg_layerpath = None
  try:
    arg_layerpath = sys.argv[1]
  except IndexError:
    pass

  finally:
    pass

  if arg_layerpath == None:
    print(f"\tMissing path to layer")
    print(f"\tExample: $ python3 {sys.argv[0]} <path to layer>")
    exit(127)
  else:
    #print(f"Checking layer file at {arg_layerpath} ...")

    contents = loadYAML(arg_layerpath)
    if contents == None:
      print(f"Could not load contents of layer {arg_layerpath} .. Terminating.")
      exit(126)
    else:
      print(f"Loaded {contents['meta']['locations']} locations from layer {arg_layerpath}")
    
    result = processContents(contents, connection)


if __name__ == '__main__':
  main()
