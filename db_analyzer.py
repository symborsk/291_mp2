import sqlite3

# References
#   1: http://stackoverflow.com/questions/10648490/removing-first-appearance-of-word-from-a-string
#       Used to get the actual table name


tables = list()
schemas = dict()
dependancies = dict()

# Method to get a database to normalize from user
def getDB():
    while True:
        # Get a filename
        print("Welcome to the Database Normalizer Program.\nPlease enter the name of the database you would like to normalize:")
        dbName = raw_input(">> ")

        # Attempt to connect to DB
        try:
            global conn
            global cursor
            conn = sqlite3.connect(dbName)
            cursor = conn.cursor()
            return
        except:
            print("Entered an invalid file name")

# Method to populate tables & schemas variables
def getInfo():
    # Get data for all tables in DB
    sql = "SELECT * FROM SQLITE_MASTER WHERE type='table'"
    cursor.execute(sql)

    # Loop over all tables to add table and schema
    for result in cursor.fetchall():
        name = result[1].replace('Input_', "", 1) # Reference 1
        tables.append(name)
        sql = result[4]
        cols = sql.split('(')[1].split(')')[0]
        lines = [line.strip().split(' ')[0] for line in cols.split(',')]
        schemas[name] = lines

    return tables, schemas

# Method to populate dependancies dictionary
def getDependancies(tables):
    for table in tables:
        # Only deal with dependancy tables
        if table.split("_")[0].lower() != "fds":
            continue
        else:
            # Get all dependancies
            sql = "SELECT * FROM {}".format("Input_" + table)
            cursor.execute(sql)

            # Add all dependancies
            tmpdict = dict()
            for result in cursor.fetchall():
                tmpdict[tuple(result[0].split(','))] = result[1].split(',')
            dependancies[table] = tmpdict

            return dependancies

getDB()
tables, schemas = getInfo()
dependancies = getDependancies(tables)
