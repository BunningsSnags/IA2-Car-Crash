"""Example of grouping commands and sqlite3"""
import sqlite3
import csv
from pathlib import Path
import click
from flask import Flask, g, current_app, render_template
from flask.cli import AppGroup


APP = Flask(__name__)
DATA_CLI = AppGroup('data', help="commands for managing data accounts")
CRASHCSV = 'data/crashData-CSV'
MAPCSV = 'data/mapData-CSV'
CRASHDATABASE = 'data/crashData.db'
MAPDATABASE = 'data/mapData.db'
APP.config['CRASHDATABASE'] = CRASHDATABASE
APP.config['MAPDATABASE'] = MAPDATABASE


def isfile(file):
    """check whether the file actually exists, relative to instance folder"""
    dbfile = Path(file)
    return dbfile.is_file()

def get_db():
    """return a database connection object"""
    if 'dbf' not in g:
        g.dbf = sqlite3.connect(
            current_app.config['CRASHDATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.dbf.row_factory = sqlite3.Row
    return g.dbf

def close_db():
    """close the connection to the database"""
    dbf = g.pop('dbf', None)

    if dbf is not None:
        dbf.close()

def initdb_crash():
    """create the database table and populate with default data"""
    try:
        conn = sqlite3.connect(CRASHDATABASE)
        if conn:
            conn.executescript("""DROP Table IF EXISTS data;
                                CREATE TABLE data 
                                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                Crash_Year TEXT, 
                                Crash_Police_Region TEXT,
                                Crash_Severity TEXT,
                                Involving_Drink_Driving TEXT,
                                Involving_Driver_Speed TEXT,
                                Involving_Fatigued_Driver TEXT,
                                Involving_Defective_Vehicle TEXT,
                                Count_Crashes TEXT,
                                Count_Fatality TEXT,
                                Count_Hospitalised TEXT,
                                Count_Medically_Treated TEXT,
                                Count_Minor_Injury TEXT,
                                Count_All_Casualties TEXT);
                                """)
            conn.commit()
    except sqlite3.DatabaseError as err:
        print("Initialising Database error\n", err)
    if conn:
        conn.close()

def upload_crash(filename):
    """upload users from a csv file if the data is valid"""
    message = None
    conn = None
    if isfile(filename):
        # ouputtuple[0] - True or False
        # ouputtuple[1] - message for user
        # ouputtuple[2] - a list of tuples containing the data
        outputtuple = isvaliddata(filename)
        message = outputtuple[1]
        if outputtuple[0]:
            try:
                # conn = sqlite3.connect(DATABASE)
                conn = get_db()
                if conn:
                    conn.executemany("""INSERT INTO data (
                                        Crash_Year, 
                                        Crash_Police_Region,
                                        Crash_Severity,
                                        Involving_Drink_Driving,
                                        Involving_Driver_Speed
                                        Involving_Fatigued_Driver,
                                        Involving_Defective_Vehicle,
                                        Count_Crashes,
                                        Count_Fatality,
                                        Count_Hospitalised,
                                        Count_Medically_Treated,
                                        Count_Minor_Injury,
                                        Count_All_Casualties) VALUES 
                                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", outputtuple[2])
                    conn.commit()
            except sqlite3.DatabaseError as err:
                print("Data Upload error\n", err)
                print(outputtuple[2])
                message = f"Error occurred uploading {filename}."
    else:
        message = f"{filename} is not a file"
    close_db()
    if message:
        print(message)



def isvaliddata(file):
    """check whether data in csv file is valid for the database

        Returns:
            A tuple.  The tuple contains three elements
                [0] - Boolean, True if data is valid, False if not
                [1] - String, Message for user
                [2] - list of tuples containing the data from csv file
    """
    with open(file, mode='r', encoding='utf8') as csvfile:
        csvdata = csv.reader(csvfile)
        data = list()
        return (True, "short cut solution", list(csvdata))
        message = "The data is not valid:\n"
        valid = True
        for num, row in enumerate(csvdata):
            if num == 0:
                continue
            if not row[0].isalpha():
                message += f"row {num} - {row[0]}\n"
                valid = False
            elif valid:
                data.append(tuple([row[0]]))
        if valid:
            message = "The data is valid."
        else:
            data = None
        return (valid, message, data)

APP.cli.add_command(DATA_CLI)


""" Pages and Routes """
@APP.route('/')
def home():
    initdb_crash()
    return render_template('home.html')

@APP.route('/crashes')
def crash():
    upload_crash('data/crashData-CSV.csv')
    return render_template('crashes.html')

@APP.route('/map')
def map():
    return render_template('map.html')

@APP.route('/report')
def report():
    return render_template('report.html')

if __name__ == "__main__":
    APP.run(debug=True)