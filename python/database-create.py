#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import datetime
import re
import sqlite3
from sqlite3 import Error

import yaml

APP_NAME = 'SHI-DatabaseCreate'
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

def removeDatabaseFile(filepath):
  if os.path.isfile(filepath):
    os.unlink(filepath)
    print(f"Deleted {filepath} as it existed")


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



def main():
  #print(f"{APP_NAME} {APP_VERSION}")

  database_path = './database.sqlite'
  print(f"Database path: {database_path}")

  removeDatabaseFile(database_path)
  connection = create_connection(database_path)
  if connection != None:
    pass

  # Create Table
  elements = []
  elements.append("CREATE TABLE IF NOT EXISTS locations (")

  for field_key in COORDINATES_SCHEMA.keys():
    field_obj = COORDINATES_SCHEMA[field_key]

    field_name = field_obj['columnName']
    field_type = field_obj['columnType']
    field = f"{field_name} {field_type}, "
    elements.append(field)

  elements.append("PRIMARY KEY (longitude,latitude,layertype)")
  elements.append(") WITHOUT ROWID;")

  query = "".join(elements)
  query = re.sub(r"\x2c\x20\x29", ")", str(query), flags=re.IGNORECASE)

  try:
    cursor = connection.cursor()
    cursor.execute(query)
  except Error as e:
    print(e)

  #print(query)
  return

if __name__ == '__main__':
  main()
