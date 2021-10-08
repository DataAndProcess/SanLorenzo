#!/usr/bin/python3
import time
import os
import datetime
import threading
import mysql.connector
import sqlite3
import redis
import json
import numpy as np
import sys

# lista file in db sqlite
# select in ordine del file da lavorare
# printa di 5 in 5 secondi con uno sleep in mezzo
# segna file come lavorato

def create_connection():
    conn = None
    try:
        conn = mysql.connector.connect(
            host="192.168.122.55",
            user="SailADV",
            password="test",
            database="SailADV",
            connect_timeout=100
        )
    except mysql.connector.Error as e:
        print(print(e)
    return conn

def insert_data_in_fw(dat):
    try:
        conn = create_connection()
        sql = ''' INSERT IGNORE INTO FolderW(FileName,Status)
                VALUES(?,?) '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
    except mysql.connector.ProgrammingError as er:
        conn.rollback()
        if not "Duplicate" in str(er):
            print(er)  # print error only if is different to UNIQUE constraint failed
    finally:
        conn.close()
    return cur.lastrowid


def lock_file(dat):
    try:
        conn = create_connection()
        sql = ''' UPDATE FolderW SET Status = 'working' WHERE FileName = ? '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
        conn.close()
    except mysql.connector.ProgrammingError as er:
        conn.rollback()
        print(er)
    finally:
        conn.close()
    return cur.lastrowid


def file_worked(dat):
    print("worked: " + str(dat[0]))
    try:
        conn = create_connection()
        sql = ''' UPDATE FolderW SET Status = 'worked' WHERE FileName = ? '''
        cur = conn.cursor()
        # print(dat)
        cur.execute(sql, dat)
        conn.commit()
        conn.close()
    except mysql.connector.ProgrammingError as er:
        conn.rollback()
        print(er)
    finally:
        conn.close()
    return cur.lastrowid


def timestamp_in_unix(timest):
    # Convert to unix time
    ts = datetime.datetime.strptime(str(timest), "%Y-%m-%dT%H:%M:%S.%fZ")
    ts = datetime.datetime.timestamp(ts)
    ts = int(float(ts))
    return ts


def generate_csv_if_not_exists():
    # create dictionary
    tmpDict = np.array([{}])
    try:
        conn = create_connection()
        sql = "SELECT `Key` FROM ToProcess"
        cur = conn.cursor()
        cur.execute(sql)
        dictionaryElements = cur.fetchall()
        conn.close()

        for d in dictionaryElements:
            tmpDict[0][str(d[0])+"_"+d[1]] = []
    except mysql.connector.Error as e:
        conn.rollback()
        conn.close()
        print(e)

    return tmpDict


def generate_table_if_not_exists():
    pass