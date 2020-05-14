"""Example of grouping commands and sqlite3"""
import sqlite3
import csv
from pathlib import Path
import click
from flask import Flask, g, current_app, render_template
from flask.cli import AppGroup


APP = Flask(__name__)
DATA_CLI = AppGroup('data', help="commands for managing data accounts")
FACTORCSV = 'data/crashData-CSV'
MAPCSV = 'data/mapData-CSV'
FACTORDATABASE = 'data/crashData.db'
MAPDATABASE = 'data/mapData.db'
APP.config['FACTORDATABASE'] = FACTORDATABASE
APP.config['MAPDATABASE'] = MAPDATABASE


""" --------------------------- Pages and Routes ----------------------------------- """
@APP.route('/')
def home():
    initdb_factor()
    initdb_map()
    return render_template('home.html')

@APP.route('/factorList')
def factorList():
    upload_factor('data/crashData-CSV.csv')
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

@APP.route("/factorList/<int:IDcrash>")
def factor(IDcrash):
    conn = get_factordb()
    crashRows = conn.execute("SELECT * FROM data WHERE id =?;", (IDcrash,))
    crashInfo = []
    for col in crashRows:
        crashInfo.append(list(col))

    return render_template("factor.html", columns=crashInfo, IDcrash = IDcrash)

"""------------------------------------------------------------------------------------------------------------------------------"""

@APP.route('/locationList')
def locationList():
    upload_map('data/mapData-CSV.csv')
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

@APP.route("/locationList/<int:IDmap>")
def location(IDmap):
    conn = get_mapdb()
    mapRows = conn.execute("SELECT * FROM data WHERE id =?;", (IDmap,))
    mapInfo = []
    for col in mapRows:
        mapInfo.append(list(col))

    return render_template("location.html", columns=mapInfo, IDmap = IDmap)

@APP.route('/help')
def help():
    return render_template('help.html')

@APP.route('/report')
def report():
    return render_template('report.html')

@APP.route('/bruh')
def bruh():
    return render_template('bruh.html')


""" ----------------------------------- Functions ----------------------------------- """
def isfile(file):
    """check whether the file actually exists, relative to instance folder"""
    dbfile = Path(file)
    return dbfile.is_file()

def get_factordb():
    """return a database connection object"""
    if 'dbf' not in g:
        g.dbf = sqlite3.connect(
            APP.config['FACTORDATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.dbf.row_factory = sqlite3.Row
    return g.dbf

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

def initdb_factor():
    """create the database table and populate with default data"""
    try:
        conn = sqlite3.connect(FACTORDATABASE)
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

def upload_factor(filename):
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
                conn = get_factordb()
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

def initdb_map():
    """create the database table and populate with default data"""
    try:
        conn = sqlite3.connect(MAPDATABASE)
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

def upload_map(filename):
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
                # conn = sqlite3.connect(DATABASE)
                conn = get_mapdb()
                if conn:
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
                                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", mapData)
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