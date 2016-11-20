import sqlite3
import itertools
import time
from db_analyzer import getClosure

tables = list()
schemas = dict()
dependancies = dict()

global conn 
global cursor 
conn = sqlite3.connect("hello.db")
cursor = conn.cursor()

def computeMinimalCoverage(schema, fds):
	removeRedundantLHSFds(fds)
	removeRedudantFds(fds)
	for key in fds:
		#TODO: if there is not super key find the key and create an add an empty dict entry to key

	print fds
	print "DONE"

def removeRedudantFds(fds):

	for key in fds:
		priorValue = fds[key]
		for fd in priorValue:
			#each value in our tuple lfs corresponds to a fd so removing it is like removing an fd
			fds[key] = tuple_without(priorValue, fd) 
			entail = tuple(getClosure(None, key, fds))

			#If we can not infer fd from the 
			if(fd  not in entail):
				fds[key] = priorValue
				

def removeRedundantLHSFds(fds):

	for key in fds:
		#Get the size and value
		value = fds[key]
		size = len(key)

		#Only possible to have redudant ones if the size is greater then one
		if(size > 1):

			for i in range(1, size):
				possibleSubSets = list(itertools.combinations(key, i))
				if(removeRedudantLhs(key, possibleSubSets, fds)):
					print
					removeRedundantLHSFds(fds)
					return

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
				lhs = tuple(sorted(result[0].split(',')))
				rhs = sorted(result[1].split(','))
				tmpdict[lhs] = rhs

            dependancies[table] = tmpdict
	return dependancies

#returns true if 
def removeRedudantLhs(key, possibleSubsets, fds):
	oldValue = fds[key]
	

	for newKey in possibleSubsets:
		newValue = tuple(getClosure(None, newKey, fds))

		#If our possible subset keys contain same closure we can replace it
		if set(oldValue).issubset(newValue):
			if newKey in fds:
				fds[newKey] = fds.pop(key)+fds[newKey]
			else:
				fds[newKey] = fds.pop(key)

			return True

	return False

def tuple_without(original_tuple, element_to_remove):
    new_tuple = []
    for s in list(original_tuple):
        if not s == element_to_remove:
            new_tuple.append(s)
    return tuple(new_tuple)


tables, schemas = getInfo()
dependancies = getDependancies(tables)
computeMinimalCoverage(None, dependancies[tables[1]])

