import sys
import sqlite3
import itertools
import copy

# References
#   1: http://stackoverflow.com/questions/10648490/removing-first-appearance-of-word-from-a-string
#       Used to get the actual table name


tables = dict()
fds = dict()

# Method to get a database to normalize from user
def getDB():
	while True:
		# Get a filename
		dbName = getInput("\nPlease enter the name of the database you would like to connect to:")

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
		if result[1][0:10].lower()=="input_fds_":
			fds[result[1][10:]] = result[1]
		elif result[1][0:6].lower()=="input_":
			name = result[1][6:]
			
			# Dont read the FD tables
			if name.split("_")[0].lower() == "fds":
				continue
			sql = result[4]
			cols = sql.split('(')[1].split(')')[0]
			lines = list()
			types = dict()
			for line in cols.split(','):
				lines.append(line.strip().split()[0])
				types[line.strip().split()[0]] = line.strip().split()[1]
			tables[name] = dict()
			tables[name][0] = lines
			tables[name][2] = types

# Method to populate dependancies dictionary
def getDependancies():
	for fd in fds:
		# Get all dependancies
		sql = "SELECT * FROM {}".format(fds[fd])
		cursor.execute(sql)

		# Add all dependancies
		tmpdict = dict()
		for result in cursor.fetchall():
			lhs = tuple(sorted(result[0].split(',')))
			rhs = set(sorted(result[1].split(',')))
			try:
				tmpdict[lhs] = tmpdict[lhs].union(rhs)
			except KeyError:
				tmpdict[lhs] = rhs

		tables[fd][1] = tmpdict

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
			newname = "Output_"+table+"_"+"".join(sorted(decomp[table][1]))
			decomp[newname] = decomp.pop(table)
			#Show decomp also puts the dependencies in valid format to be user in our put into table
			fds = showDecomp(decomp)
			checkPreservation(tables[table][1], decomp)
			#Specifically added genereate all the fds in on dictionary
		
			putIntoTable(fds, table)
			return 

		currFDs = decomp[currTable][0]
		currSchema = decomp[currTable][1]

		lhs, rhs = getInvalidFD(currTable, currFDs, currSchema)
		newschema = set(lhs).union(rhs)
		currSchema = set(currSchema).difference(rhs)
		newfds = getFDs(newschema, currFDs)
		updateFDs(currSchema, currFDs)
		newname = "Output_" + table + "_" + "".join(sorted(newschema))
		if currTable[0:7]=="Output_":
			rename = "Output_" + table + "_" + "".join(sorted(currSchema))
			decomp[rename] = [currFDs, currSchema]
			decomp.pop(currTable)
		else:
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
	totalFds = dict()
	for key in decomp:
		print "Table ", key
		print "Schema ", "".join(decomp[key][1])
		for dep in decomp[key][0]:
				totalFds[dep] = decomp[key][0][dep]
				print "".join(dep), " --> ", "".join(decomp[key][0][dep])
		print " "

	return totalFds

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
		for i in range(1, len(vals1)+1):
			for val in itertools.combinations(vals1, i):
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
	print ""
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
		print "\nWhat would you like to do?\nPress '.exit' at any time to quit."
		options = {"3. Get closure of an attribute",
			"2. Check set equivalency",
			"\n1. Normalize a database"}
		sel = getInput("\n".join(options))
		# Normalization
		if sel=='1':
			# Loop until user provides proper input
			waiting = True
			while waiting:
				sel = getInput("\nHow would you like to normalize? \n1. BCNF \n2. 3NF")
				if sel=='1':
					tableName =getInput("Please enter a table name:")
					decompBCNF(tableName) 
					
					if(promptToFillTables(tableName)):
						fillTables(tableName)
					waiting = False
				elif sel=='2':
					tableName = getInput("Please enter a table name:")
					decomp3nf(tableName)
					
					if(promptToFillTables(tableName)):
						fillTables(tableName)
					waiting = False
				else:
					print "Please make a valid selection."
		# Outward facing set equivalency check
		elif sel=='2':
			userCheckEquivalency()
		elif sel=='3':
			table = getInput("Please enter the name of the dependancy table:")
			atts = [x.strip() for x in getInput("Please enter a comma separated list of attributes:").split(',')]
			fds = dict()
			addFDs(table, fds)
			closure = getClosure(None, atts, fds)
			resultstr = "".join(atts) + "+ = "+ "".join(closure)
			print resultstr
		else:
			print "Please make a valid selection."

#Performs all the steps to decompose the given table into 3nf form
def decomp3nf(tableName):
	fds = tables[tableName][1]
	removeRedundantLhsFds(fds)
	removeRedudantFds(fds)
	for key in fds:
		if(isSuperKey(key, tables[tableName][1], tables[tableName][0])):
			putIntoTable(fds, tableName)
			return
		
	# if none are super keys add the key
	primaryKey = getKeys(tableName)
	fds[primaryKey[0]] = tuple()
	putIntoTable(fds, tableName)

#remove all the redudant fds for specific group, checks if the closure of a grouping still can be inferred
#after removing that explicit fd. ex remove a-->b and check if in the closure of a that  b is still there
def removeRedudantFds(fds):

	for key in fds:
		priorValue = fds[key]
		attemptToRemoveValues(priorValue, key, fds)
	
def attemptToRemoveValues(value, key, fds):
		for fd in value:
			#each value in our tuple lfs corresponds to a fd so removing it is like removing an fd
			newValue = set(tuple_without(value, fd)) 
			fds[key] = newValue
			entail = tuple(getClosure(None, key, fds))

			#If we can not infer fd from the 
			if(fd  not in entail):
				fds[key] = value
			else:
				attemptToRemoveValues(newValue, key, fds)
				return

#Runs through all the 
def removeRedundantLhsFds(fds):
	for key in fds:
		#Get the size and value
		value = fds[key]
		size = len(key)

		#Only possible to have redudant ones if the size is greater then one
		if(size > 1):

			for i in range(1, size):
				possibleSubSets = list(itertools.combinations(key, i))
				if(canRemoveValueLhsFds(key, possibleSubSets, fds)):
					removeRedundantLhsFds(fds)
					return

#Trys to remove a redudant values from the lhs and checks if the closure stays the same
# return True if you successfully remove it returns false if there nothing redundant
def canRemoveValueLhsFds(key, possibleSubsets, fds):
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

#Generic function regenerate a tuple without the element_to_remove
def tuple_without(original_tuple, element_to_remove):
	new_tuple = []
	for s in list(original_tuple):
		if not s == element_to_remove:
			new_tuple.append(s)
	return tuple(new_tuple)

#Puts an output either bncf or 3nf into an actualy output table
def putIntoTable(fds, nameOfTable):
	#Generate schema table
	for fdKey in fds:
		fdVal = fds[fdKey]

		isSubset, keyToInsertInto = isSchemaSubset(fds, fdKey)

		#SO we get the simplest tables we do not want to create new tables for ones that are subsets
		fdTableName = str()
		if(not isSubset):
			schemaTableName = "Output_" + nameOfTable + "_" + str(generateOutputString(fdKey, fds))
			cursor.execute("Drop Table if exists {}".format(schemaTableName))
			conn.commit()
			
			sqlSchema = generateCreateTableQuery(fdKey, fdVal, schemaTableName, nameOfTable)
			cursor.execute(sqlSchema)
			conn.commit()

			fdTableName = "Output_FDS_" + nameOfTable + "_" + str(generateOutputString(fdKey, fds))
			cursor.execute("Drop Table if exists {}".format(fdTableName))
			conn.commit()

			sqlFd = "Create Table {}(LHS TEXT, RHS TEXT)".format(fdTableName)
			cursor.execute(sqlFd)

		#Otherwise we want to take the key to insert into and insert our already made fds into that
		else:
			fdTableName = "Output_FDS_" + nameOfTable + "_" + str(generateOutputString(keyToInsertInto, fds))

		insertFdsIntoDb(fdKey, fdVal, fdTableName)
		conn.commit()

#Generates schema from fds and key
def generateSchema(value, key):
	new_tuple = []
	for key in key:
		new_tuple.append(key)
	for val in value:
		new_tuple.append(val)

	return set(tuple(new_tuple))

#Determines if the schema given is a subset of another schema
def isSchemaSubset(fds, givenKey):
	
	schema = generateSchema(fds[givenKey], givenKey)
	for key in fds:
		#Obviously its own schema is a subset of itself
		if key == givenKey:
			continue
		
		temp_set = generateSchema(fds[key], key)

		# We want to see if it is not equal and subset
		if(schema.issubset(temp_set)):
			return True, key

	return False, givenKey

#Generates all the columns in a specific grouping
def generateOutputString(fdKey, fds):
	
	fdValue = fds[fdKey]
	tempString = str()
	for key in fdKey:
		tempString += str(key)
	for val in fdValue:
		tempString += str(val)
	return tempString

#Generates a generic create table query for a grouping with LHS fdKey and RHS of fdValue
#Output name in the name of table being made inputName in the name of table eg "R1" that is in the input table
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

#Inserts into a specific table of name the fd grouping 
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

#A user prompt to ask them if they would like to fill in values for output tables
def promptToFillTables(tableName):
	
	while True:
		userInput = getInput("Would you like to fill output tables " + tableName + " (Y/N)")
		if(userInput.lower() == "y"):
			return True
		elif(userInput.lower() =="n"):
			return False
		print("Invalid input please try again")

#for filling Output_R tables from Input_R tables
def fillTables(tableName):
	sql = "SELECT * FROM SQLITE_MASTER WHERE type='table'"
	cursor.execute(sql)
	for result in cursor.fetchall():
		#get the letters we want information for
		if "Output_FDS" in result[1]:
			continue
		elif("Output_" in result[1]):
			name = result[1].replace("Output_", "", 1)
			for x in range(2,len(name)):
				if (name[x] == "_"):
					new_form = name[x+1:]
					nameofForm = name[:x]

			sql2 = "SELECT * FROM SQLITE_MASTER WHERE type='table'"
			cursor.execute(sql2)

			# get info from input, all letters are stored into listoflists, in order read
			for resultz in cursor.fetchall():
				if (("input_") not in resultz[1].lower()):
					continue
				if (nameofForm not in resultz[1]):
					continue
				if ("fds" in resultz[1].lower()):
					continue
				if( nameofForm != tableName):
					continue

				lol = []
				lol2 = []
				lol3 = []
				for y1 in range(len(new_form)):
					lol.append((str)(new_form[y1]))
						
				for y in range(len(new_form)):
					sql5 = ("SELECT {} FROM {}".format( (str) (lol[y]), resultz[1]))
					cursor.execute(sql5,)
					values5 = cursor.fetchall()
					lol2.append(values5)

				for z in range(len(lol2[0])):
					newlist = []
					for w in range(len(lol2)):
						newlist.append((str)(((lol2[w])[z])[0]))
					lol3.append(newlist)
							
				stringQuery = ""
				check = []
				for ins in range(len(lol3)):
					ins3 = ((lol3[ins]))
					sql3 = (InsertInto(ins3,result[1]))
					nl = []
					for val in range(len(ins3)):
						nv = (str)(ins3[val])
						nl.append(nv)
					if nl in check:
						continue
					check.append(nl)
					params = tuple(nl)
					cursor.execute(sql3, params)
					conn.commit()												

#Generates a insert into of the correct number of values in listOfValue
def InsertInto(listOfValue, tableName):
	stringQuery = "Insert into {} VALUES (".format(tableName)

	for val in listOfValue:
		stringQuery += "?,"

	stringQuery = stringQuery[:-1]
	stringQuery += ")"
	return stringQuery

applicationMenu()
