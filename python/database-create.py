#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import datetime

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
  'title': {
    'columnName': 'title',
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
  print(f"{APP_NAME} {APP_VERSION}")

  if sys.argv[1] != "":
    if len(str(sys.argv[1])) > 7:
      database_path = sys.argv[1]
      print(f"Database path: {database_path}")

      removeDatabaseFile(database_path)
      connection = create_connection(database_path)
      if connection != None:
        pass

    else:
      print(f"Invalid parameter")
      print(f"$ python3 {sys.arg[0]} ./database.sqlite")
      exit(127)
  else:
    print(f"$ python3 {sys.arg[0]} ./database.sqlite")
    exit(126)

if __name__ == '__main__':
  main()
