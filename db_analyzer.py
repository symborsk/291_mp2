import sqlite3
import itertools

# References
#   1: http://stackoverflow.com/questions/10648490/removing-first-appearance-of-word-from-a-string
#       Used to get the actual table name


tables = list()
FDs = list()
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
        if name.split("_")[0].lower() == "fds":
            FDs.append(name)
            continue
        tables.append(name)
        sql = result[4]
        cols = sql.split('(')[1].split(')')[0]
        lines = [line.strip().split(' ')[0] for line in cols.split(',')]
        schemas[name] = lines

    return tables, schemas

# Method to populate dependancies dictionary
def getDependancies(tables):
    for table in FDs:
        # Get all dependancies
        sql = "SELECT * FROM {}".format("Input_" + table)
        cursor.execute(sql)

        # Add all dependancies
        tmpdict = dict()
        for result in cursor.fetchall():
            lhs = tuple(sorted(result[0].split(',')))
            rhs = sorted(result[1].split(','))
            tmpdict[lhs] = rhs

        dependancies[table] = tmpdict

    return dependancies

# Method to get the closure of rhs set
def getClosure(closure, rhs, dependancies):
	# Initialize the closure if necessary & determine length
	if (closure==None):
		closure = set(rhs)
	length = len(closure)

	# Loop through all LHS and see if we can append to closure
	for dep in dependancies:
		if closure.issuperset(dep) and closure.issuperset(dependancies[dep])==False:
			closure = closure.union(dependancies[dep])
			closure = set(sorted(closure))

	# Recurse if necessary
	if len(closure)==length:
		return closure
	else:
		return getClosure(closure, rhs, dependancies)

def getSuperKeys(table):
	cols = schemas[table]
	superkeys = list()
	# Try all possible combinations of columns to create superkeys
	for i in range(0, len(cols), 1):
		for j in list(itertools.combinations(cols, i)):
			if (len(getClosure(None, j, dependancies["FDs_"+table]))==len(cols)):
				superkeys.append(j)
	return superkeys

def isSuperKey(key, table):
    cols = schemas[table]
    return len(getClosure(None, key, dependancies["FDs_"+table]))==len(cols)


def checkBCNF(table, dependancies):
    for dep in dependancies:
        if not dep.issuperset(dependancies[dep]) or not isSuperKey(dep, table):
            return False

    return True


getDB()
tables, schemas = getInfo()
dependancies = getDependancies(tables)
print(getSuperKeys("R1"))
