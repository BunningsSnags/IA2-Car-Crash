"""Example of grouping commands and sqlite3"""
import sqlite3
import csv
from pathlib import Path
import click
from flask import Flask, g, current_app, render_template
from flask.cli import AppGroup


APP = Flask(__name__)
DATA_CLI = AppGroup('data', help="commands for managing data accounts")
FACTORCSV = 'CSV Factor Name (Without .csv)' # change variable names to what you wish
MAPCSV = 'CSV Location name (Without .csv)' # change variable names to what you wish
FACTORDATABASE = 'DATABASE name you want' # change variable names to what you wish
MAPDATABASE = 'DATABASE name you want' # change variable names to what you wish


""" --------------------------- Pages and Routes ----------------------------------- """
@APP.route('/')
def home():
    initdb_factor() # your init table function (factorstable)
    initdb_map() # your init table function (locationstable)
    return render_template('home.html')

# Change Variable Names
@APP.route('Path to factor list')
def factorList():
    upload_factor('Name of location database variable')
    dataout = []
    crashID = []
    try:
        conn = get_factordb()
        if conn:
            rows = conn.execute("SELECT*FROM data;")
            for row in rows:
                dataout.append(row)
                crashID.append(int(row[0]))
    except sqlite3.DatabaseError as err:
        print("Error\n", err)
    close_db()
    return render_template('factorList.html', getcrashdata = dataout, IDcrash = crashID)

"""------------------------------------------------------------------------------------------------------------------------------"""

# Change Variable Names
@APP.route('Path to location list')
def locationList():
    upload_map('Name of location database variable')
    mapdataout = []
    mapID = []
    try:
        conn = get_mapdb()
        if conn:
            rows = conn.execute("SELECT*FROM data;")
            for row in rows:
                mapdataout.append(row)
                mapID.append(row[0])
    except sqlite3.DatabaseError as err:
        print("Error\n", err)
    close_db()
    return render_template('locationList.html', getmapdata = mapdataout, IDmap = mapID)


""" ----------------------------------- Functions ----------------------------------- """
def isfile(file):
    """check whether the file actually exists, relative to instance folder"""
    dbfile = Path(file)
    return dbfile.is_file()

# Change these names to what you desire
def get_factordb():
    """return a database connection object"""
    if 'dbf' not in g:
        g.dbf = sqlite3.connect(
            APP.config['FACTORDATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.dbf.row_factory = sqlite3.Row
    return g.dbf

# Change these names to what you desire
def get_mapdb():
    """return a database connection object"""
    if 'dbf' not in g:
        g.dbf = sqlite3.connect(
            APP.config['MAPDATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.dbf.row_factory = sqlite3.Row
    return g.dbf

def close_db():
    """close the connection to the database"""
    dbf = g.pop('dbf', None)

    if dbf is not None:
        dbf.close()

"""-----------------------------------------------------------------------------------------------------------"""

def initdb_factor(): # Change This Name
    """create the database table and populate with default data"""
    try:
        conn = sqlite3.connect(FACTORDATABASE) # Change This Name
        if conn:
            conn.executescript("""DROP Table IF EXISTS data;
                                CREATE TABLE data 
                                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                Crash_Year INT, 
                                Crash_Police_Region TEXT,
                                Crash_Severity TEXT,
                                Involving_Drink_Driving TEXT,
                                Involving_Driver_Speed TEXT,
                                Involving_Fatigued_Driver TEXT,
                                Involving_Defective_Vehicle TEXT,
                                Count_Crashes INT,
                                Count_Fatality INT,
                                Count_Hospitalised INT,
                                Count_Medically_Treated INT,
                                Count_Minor_Injury INT,
                                Count_All_Casualties INT);
                                """)
            conn.commit()
    except sqlite3.DatabaseError as err:
        print("Initialising Database error\n", err)
    if conn:
        conn.close()

def upload_factor(filename): # Change This Name
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
                conn = get_factordb() # Change This Name
                if conn:
                    conn.executemany("""INSERT INTO data (
                                        Crash_Year, 
                                        Crash_Police_Region,
                                        Crash_Severity,
                                        Involving_Drink_Driving,
                                        Involving_Driver_Speed,
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

"""--------------------------------------------------------------------------------------------------------------------------"""

def initdb_map(): # Change This Name
    """create the database table and populate with default data"""
    try:
        conn = sqlite3.connect(MAPDATABASE) # Change This Name
        if conn:
            conn.executescript("""DROP Table IF EXISTS data;
                                CREATE TABLE data 
                                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                                Crash_Severity TEXT,
                                Crash_Year INT,
                                Crash_Month INT,
                                Crash_Day_Of_Week INT,
                                Loc_Suburb TEXT,
                                Loc_Post_Code INT,
                                Loc_Police_Division TEXT,
                                Loc_Police_District TEXT,
                                Loc_Police_Region TEXT,
                                Count_Casualty_Fatality INT,
                                Count_Casualty_Hospitalised INT,
                                Count_Casualty_MedicallyTreated INT,
                                Count_Casualty_MinorInjury INT,
                                Count_Casualty_Total INT);
                                """)
            conn.commit()
    except sqlite3.DatabaseError as err:
        print("Initialising Database error\n", err)
    if conn:
        conn.close()

def upload_map(filename): # Change This Name
    """upload users from a csv file if the data is valid"""
    message = None
    conn = None
    if isfile(filename):
        # ouputtuple[0] - True or False
        # ouputtuple[1] - message for user
        # ouputtuple[2] - a list of tuples containing the data
        outputtuple = isvaliddata(filename)
        message = outputtuple[1]
        mapData = list()
        if outputtuple[0]:
            try:
                conn = get_mapdb() # Change This Name
                if conn:
                    # See if you can change some of these names, if not, thats fine
                    for row in outputtuple[2]:
                        getCol = [1,2,3,4,13,15,16,17,18,int(40),int(41),int(42),int(43),int(44)]
                        newrow = list()
                        for col in getCol:
                            newrow.append(row[col])
                        mapData.append(tuple(newrow))

                    conn.executemany("""INSERT INTO data (
                                        Crash_Severity,
                                        Crash_Year,
                                        Crash_Month,
                                        Crash_Day_Of_Week,
                                        Loc_Suburb,
                                        Loc_Post_Code,
                                        Loc_Police_Division,
                                        Loc_Police_District,
                                        Loc_Police_Region,
                                        Count_Casualty_Fatality,
                                        Count_Casualty_Hospitalised,
                                        Count_Casualty_MedicallyTreated,
                                        Count_Casualty_MinorInjury,
                                        Count_Casualty_Total) VALUES 
                                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", mapData) # Change This Name
                    conn.commit()
            except sqlite3.DatabaseError as err:
                print("Data Upload error\n", err)
                # print(outputtuple[2])
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
                [3] - map data
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

if __name__ == "__main__":
    APP.run(debug=True)