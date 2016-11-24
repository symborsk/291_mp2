import sys
import sqlite3
import itertools
import copy

# References
#   1: http://stackoverflow.com/questions/10648490/removing-first-appearance-of-word-from-a-string
#       Used to get the actual table name


tables = dict()

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
		if result[1][0:6]=="Input_":
			name = result[1].replace('Input_', "", 1) # Reference 1
			if name.split("_")[0].lower() == "fds":
				continue
			sql = result[4]
			cols = sql.split('(')[1].split(')')[0]
			lines = list()
			types = dict()
			for line in cols.split(','):
				lines.append(line.strip().split(' ')[0])
				types[line.strip().split(' ')[0]] = line.strip().split(' ')[1]
			tables[name] = dict()
			tables[name][0] = lines
			tables[name][2] = types

# Method to populate dependancies dictionary
def getDependancies():
	for name in tables:
		# Get all dependancies
		sql = "SELECT * FROM {}".format("Input_FDs_" + name)
		cursor.execute(sql)

		# Add all dependancies
		tmpdict = dict()
		for result in cursor.fetchall():
			lhs = tuple(sorted(result[0].split(',')))
			rhs = set(sorted(result[1].split(',')))
			tmpdict[lhs] = rhs

		tables[name][1] = tmpdict

# Method to get the closure of rhs set
def getClosure(closure, lhs, dependancies):
	# Initialize the closure if necessary & determine length
	if (closure==None):
		closure = set(lhs)
	length = len(closure)

	# Loop through all LHS and see if we can append to closure
	for dep in dependancies:
		if closure.issuperset(dep) and not closure.issuperset(dependancies[dep]):
			closure = set(sorted(closure.union(dependancies[dep])))

	# Recurse if necessary
	if len(closure)==length:
		return closure
	else:
		return getClosure(closure, lhs, dependancies)

def getKeys(table):
	superkeys = list()
	# Try all possible combinations of columns to create superkeys
	for i in range(0, len(tables[table][0]), 1):
		for j in list(itertools.combinations(tables[table][0], i+1)):
			if (len(getClosure(None, j, tables[table][1]))==len(tables[table][0])):
				superkeys.append(j)

	# 1st instance in superkeys will always have min length
	minLength = len(superkeys[0])
	result = list()
	for key in superkeys:
		if len(key)==minLength:
			result.append(key)
	return result

def isSuperKey(key, dependancies, schema):
	a = getClosure(None, key, dependancies)
	return len(a)==len(schema)


# Checks if the given table is in BCNF format
def checkBCNF(dependancies, schema):
	for dep in dependancies:
		if not set(dep).issuperset(dependancies) and not isSuperKey(dep, dependancies, schema):
			return False

	return True

def getInvalidTable(decomp):
	for table in decomp:
		if not checkBCNF(decomp[table][0], decomp[table][1]):
			return table
	return -1

# Returns the first invalid FD in the table
def getInvalidFD(table, dependancies, schema):
	# Ideally we first want to remove FDs which don't impact other FDs
	for dep in dependancies:
		if not set(dep).issuperset(dependancies[dep]) and not isSuperKey(dep, dependancies, schema) and not dependancies[dep] in dependancies.keys():
			return dep, dependancies[dep]

	# Get any invalid FD
	for dep in dependancies:
		if not set(dep).issuperset(dependancies[dep]) and not isSuperKey(dep, dependancies, schema):
			return dep, dependancies[dep]

	return -1, -1

def getFDs(newschema, dependancies):
	newfds = dict()
	for dep in dependancies:
		curr = set(dep).union(dependancies[dep])
		if curr.issubset(newschema):
			newfds[tuple(copy.deepcopy(dep))] = copy.deepcopy(dependancies[dep])
	return newfds

def updateFDs(schema, dependancies):
	for dep in dependancies.keys():
		curr = set(dep).union(dependancies[dep])
		val = dependancies.pop(dep)
		if not curr.intersection(schema)==set():
			# Re add necessary parts
			dep = set(dep).intersection(schema)
			val = set(val).intersection(schema)
			if not dep==set() and not val==set():
				dependancies[tuple(dep)] = val

def decompBCNF(table):
	decomp = dict()
	decomp[table] = [copy.deepcopy(tables[table][1]), copy.deepcopy(tables[table][0])]
	
	while True:
		currTable = getInvalidTable(decomp)
		# Finished case
		if currTable==-1:
			# Handle initial table then return
			newname = table+"_"+"".join(decomp[table][1])
			decomp[newname] = decomp.pop(table)
			showDecomp(decomp)
			checkPreservation(tables[table][1], decomp)
			return 

		currFDs = decomp[currTable][0]
		currSchema = decomp[currTable][1]

		lhs, rhs = getInvalidFD(currTable, currFDs, currSchema)
		newschema = set(lhs).union(rhs)
		currSchema = set(currSchema).difference(rhs)

		newfds = getFDs(newschema, currFDs)
		updateFDs(currSchema, currFDs)
		newname = table + "_" + "".join(newschema)
		decomp[currTable] = [currFDs, currSchema]
		decomp[newname] = [newfds, newschema]

def checkPreservation(dependancies, decomp):
	tempFDs = dict()
	preserved = True
	for table in decomp:
		for lhs in decomp[table][0]:
			try:
				tempFDs[lhs] += decomp[table][0][lhs]
			except KeyError:
				tempFDs[lhs] = decomp[table][0][lhs]

	if checkEquivalency(dependancies, tempFDs):
		print "Dependancy was preserved."
	else:
		print "Dependancy was not preserved."

def showDecomp(decomp):
	for key in decomp:
		print "Table ", key
		print "Schema ", "".join(decomp[key][1])
		for dep in decomp[key][0]:
				print "".join(dep), " --> ", "".join(decomp[key][0][dep])
		print " "

def userCheckEquivalency():
	set1 = getInput("Please enter a comma separated list of tables for the first set:")
	set2 = getInput("Please enter a comma separated list of tables for the second set:")

	set1 = set([x.strip() for x in set1.split(",")])
	fds1 = dict()
	vals1 = set()
	set2 = set([x.strip() for x in set2.split(",")])
	fds2 = dict()
	vals2 = set()

	for table1 in set1:
		addFDs(table1, fds1)

	for table2 in set2:
		addFDs(table2, fds2)

	if checkEquivalency(fds1, fds2):
		print "The two sets are equivalent."
	else:
		print "The two sets are not equivalent."

def checkEquivalency(fds1, fds2):
	vals1 = set()
	for key in fds1:
		vals1 = vals1.union(key)
		vals1 = vals1.union(fds1[key])

	vals2 = set()
	for key in fds2:
		vals2 = vals2.union(key)
		vals2 = vals2.union(fds2[key])

	if not vals1==vals2:
		return False

	else:
		for val in vals1:
			if not getClosure(None, val, fds1)==getClosure(None, val, fds2):
				return False

	# If all elements have the same closure in both sets then they both entail each other & are equivalent
	return True


def addFDs(table, inputdict):
	sql = "SELECT * FROM {}".format(table)
	cursor.execute(sql)
	results = cursor.fetchall()

	for result in results:
		key = tuple([x.strip() for x in result[0].split(',')])
		val = [x.strip() for x in result[1].split(',')]
		inputdict[key] = val

def getInput(str):
	print str
	sel = raw_input(">>")
	if sel.lower()==".exit":
		quit()
	return sel


def applicationMenu():
	print "\nWelcome to the Database Analyzer Program"

	# Attempt to connect to an inputted db
	try:
		global conn
		global cursor
		if sys.argv[1][-3:] == ".db":
			conn = sqlite3.connect(sys.argv[1])
			cursor = conn.cursor()
		else:
			getDB()
	# No input provided
	except IndexError:
		getDB()

	# Gather info on Input_ & Input_FDs tables
	getInfo()
	getDependancies()

	# Handle all user input until they exit
	while True:
		print "\n\nWhat would you like to do?\nPress '.exit' at any time to quit."
		options = {"\n1. Normalize a database",
			"2. Check set equivalency"}
		sel = getInput("\n".join(options))
		# Normalization
		if sel=='1':
			# Loop until user provides proper input
			waiting = True
			while waiting:
				sel = getInput("\nHow would you like to normalize? \n1. BCNF \n2. 3NF")
				if sel=='1':
					decompBCNF(getInput("Please enter a table name:"))
					waiting = False
				elif sel=='2':
					decomp3nf(getInput("Please enter a table name:"))
					waiting = False
				else:
					print "Please make a valid selection."
		elif sel=='2':
			userCheckEquivalency()
		else:
			print "Please make a valid selection."

def decomp3nf(tableName):
	fds = tables[tableName][1]
	removeRedundantLHSFds(fds)
	removeRedudantFds(fds)
	for key in fds:
		if(isSuperKey(key, tables[tableName][1], tables[tableName][0])):
			return
		
	# if none are super keys add the key
	primaryKey = getKeys(tableName)
	fds[primaryKey[0]] = tuple()
	put3nfIntoTable(fds, "R1")

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
					removeRedundantLHSFds(fds)
					return

#returns true if 
def removeRedudantLhs(key, possibleSubsets, fds):
	oldValue = fds[key]
	

	for newKey in possibleSubsets:
		newValue = tuple(getClosure(None, newKey, fds))

		#If our possible subset keys contain same closure we can replace it
		if set(oldValue).issubset(newValue):
			if newKey in fds:
				fds[newKey] = fds[newKey].union(fds.pop(key))
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

def put3nfIntoTable(fds, nameOfTable):

	#Generate schema table
	for fdKey in fds:
		fdVal = fds[fdKey]
		schemaTableName = "Output_" + nameOfTable + "_" + str(generateOutputString(fdKey, fdVal))
		sqlSchema = generateCreateTableQuery(fdKey, fdVal, schemaTableName, nameOfTable)
		cursor.execute(sqlSchema)
		conn.commit()

		fdTableName = "Output_FDS_" + nameOfTable + "_" + str(generateOutputString(fdKey, fdVal))
		sqlFd = "Create Table {}(LHS TEXT, RHS TEXT)".format(fdTableName)
		cursor.execute(sqlFd)
		insertFdsIntoDb(fdKey, fdVal, fdTableName)
		conn.commit()

def generateOutputString(fdKey, fdValue):
	tempString = str()
	for key in fdKey:
		tempString += str(key)
	for val in fdValue:
		tempString += str(val)
	return tempString

def generateCreateTableQuery(fdKey, fdValue, outputName, inputName):
	tempQuery = "Create Table {}(".format(outputName)
	primaryKeyQuery = "primary key("
	for key in fdKey:
		variableType = tables[inputName][2][key]
		tempQuery += str(key) + " "+ variableType + ","
		primaryKeyQuery += key + ","
	for val in fdValue:
		variableType = tables[inputName][2][val]
		tempQuery += str(val) +" "+variableType + ","

	#slice off the last comma as it it not neccessary
	primaryKeyQuery = primaryKeyQuery[:-1]
	tempQuery += primaryKeyQuery
	tempQuery += "))"

	return tempQuery

def insertFdsIntoDb(fdKey, fdValue, name):
	lhs = str()
	rhs = str()

	for key in fdKey:
		lhs += key + ","
	for val in fdValue:
		rhs += val + ","

	#Remove the final comma
	lhs = lhs[:-1]
	rhs = rhs[:-1]

	sql = "Insert into {}(LHS, RHS) VALUES(?,?)".format(name)
	params = (lhs,rhs)
	cursor.execute(sql, params)

applicationMenu()
