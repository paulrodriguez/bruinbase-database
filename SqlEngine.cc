/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning
  
	RC     rc;
	int    key;     
	string value;
	int    count;
	int    diff;
    //flags to check conditions
	int valueconds = 0; //keeps track if there are conditions on values
	int keyequalityconds = 0; //keeps track if there where any equality conditions
	int keyconds = 0; //keeps track if there were any conditions on key
	bool notEqual = false; //if we use the not equal then we probably have to search the records
	bool lowerboundset = false; //if a greater than, or greater than or equal to condition is found
	bool upperboundset = false;//if a less than, or less than or equal to condition is found
	int upperbound =-1; 
	int LTE = 0; //if 0 then condition is less than, if 1 condition is less than or equal to
	int lowerbound = -1;
	int GTE = 0;//if 0 then condition is greater than, if 1 condition is greater than or equal to
	int equal_to = -1;
	
	int val;
	// open the table file
	if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}

	//to check the types of equality conditions and get the lower and upper bounds if conditions contain keys
	for (unsigned i = 0; i < cond.size(); i++) {
		//if the first condition is for a key
		if(cond[i].attr == 1) {
			val = atoi(cond[i].value);
			switch (cond[i].comp) {
				case SelCond::EQ:
					//if equality condition on same key appears twice then dont check it
					if(keyequalityconds==1 && equal_to==val) break;
					equal_to=val;
					keyequalityconds++;
					keyconds++;
					if(keyequalityconds >=2) break;
				break;
				case SelCond::GT:
					if(lowerbound <= val && lowerbound != -1) {
						lowerbound = val;
						keyconds++;
						GTE = 0;
						lowerboundset = true;
					}
					else if (lowerbound == -1) {lowerbound = val;keyconds++;GTE = 0; lowerboundset = true;}
				break;
				case SelCond::LT:
					if(upperbound >= val && upperbound!=-1) {
						upperbound = val;
						LTE=0;
						keyconds++;
						upperboundset = true;
					}
					else if(upperbound == -1){upperbound = val;keyconds++; LTE=0;upperboundset = true;}
				break;
				case SelCond::GE:
					if(lowerbound < val && lowerbound != -1) {
						lowerbound = val;
						GTE = 1;
						keyconds++;
						lowerboundset = true;
					}
					else if (lowerbound == -1) {lowerbound = val; keyconds++; GTE = 1;lowerboundset = true;}
				break;
				case SelCond::LE:
				if(upperbound > val && upperbound!=-1) {
						upperbound = val;
						LTE = 1;
						keyconds++;
						upperboundset = true;
					}
					else if(upperbound == -1){upperbound = val; keyconds++; LTE=1; upperboundset = true;}
				break;
				case SelCond::NE:
					notEqual = true;
				break;
			}	
		}
		//if we have a check condition on a key, then we will probably have to scan the entire table
		if(cond[i].attr==2) {valueconds++;}
	}

	//if only an equality condition is in the where clause and we did not use any other conditions on keys then we can set upper bound and lower bounds to the value of equality condition
	if(!lowerboundset && keyequalityconds==1) {lowerbound = equal_to; lowerboundset=true; GTE =1;}
	if(!upperboundset && keyequalityconds==1) {upperbound = equal_to; upperboundset=true;LTE =1;}
		
	rid.pid = rid.sid = 0;
	count = 0;
	//this is used to convert an a pointer to integer
	
	RC openindex;
	BTreeIndex treeindex;
	openindex=treeindex.open(table+".idx", 'r');
	//if there is an index file then use it to get the records
	if((openindex>=0 && keyconds>=1)||(attr==4 && valueconds==0)) {
	
		//if two equality condtions are in where clause, then we need to stop search
		if(keyequalityconds>=2) {treeindex.close(); goto exit_select;}
		//to read the root pid and height of tree
		treeindex.readdata();
		
		IndexCursor cursor;
		//if an equality condition, then it has priority to be checked first
		if(keyequalityconds == 1) {
			rc = treeindex.locate(equal_to, cursor);	
		}
		else {
			rc = treeindex.locate(lowerbound, cursor);	
		}
		next_entry:
		while(cursor.pid != -1) {
			treeindex.readForward(cursor, key, rid);
			
			//if key passed the upperbound then exit. if no upperbound was issued then the if else statement will not run
			if(key>equal_to && keyequalityconds==1) {treeindex.close(); goto exit_select;}
			else if(key >= upperbound && LTE == 0 && upperboundset) {treeindex.close(); goto end_select;}
			else if(key > upperbound && LTE == 1 && upperboundset) {treeindex.close(); goto end_select;}
			//if the attribute requires to print key and value or meet a condition on a value, then get the record from table file
			
			if(lowerboundset) {
				//if only an equality condition was issued, then print the tuple if 
				if(key == equal_to && keyequalityconds==1) {
					if(key > lowerbound && GTE==0) goto print_tuple;
					else if(key >= lowerbound && GTE==1) goto print_tuple;	
					else goto next_entry;
				}
				else if(key > lowerbound && GTE==0) goto print_tuple;
				else if(key >= lowerbound && GTE==1) goto print_tuple;
				else goto next_entry;
			}
			print_tuple:
			//only run if "select *", "select value" or there is a condition on value in where clause
			if (attr==2 || attr==3 || valueconds>=1) {
				if ((rc = rf.read(rid, key, value)) < 0) {
					fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
					treeindex.close();
					goto exit_select;
				}
			}	
			
			//if conditions exist for values or not equal then iterate through all conditions
			if(valueconds>=1 || notEqual) {
				for (unsigned i = 0; i < cond.size(); i++) {
					// compute the difference between the tuple value and the condition value
					switch (cond[i].attr) {
					  case 1:
					diff = key - atoi(cond[i].value);
					break;
					  case 2:
					diff = strcmp(value.c_str(), cond[i].value);
					break;
					  }
					// skip the tuple if any condition is not met
					switch (cond[i].comp) {
					case SelCond::EQ:
					if (diff != 0) goto next_entry;
					break;
					  case SelCond::NE:
					if (diff == 0) goto next_entry;
					break;
					  case SelCond::GT:
					if (diff <= 0) goto next_entry;
					break;
					  case SelCond::LT:
					if (diff >= 0) goto next_entry;
					break;
					  case SelCond::GE:
					if (diff < 0) goto next_entry;
					break;
					  case SelCond::LE:
					if (diff > 0) goto next_entry;
					break;
					}
				}
			}	
				//increase tuple count
				count++;
				
				// print the tuple 
				switch (attr) {
				case 1:  // SELECT key
				  fprintf(stdout, "%d\n", key);
				  break;
				case 2:  // SELECT value
				  fprintf(stdout, "%s\n", value.c_str());
				  break;
				case 3:  // SELECT *
				  fprintf(stdout, "%d '%s'\n", key, value.c_str());
				  break;
				}
		}
		//close B+Tree
		treeindex.close();
	}
else {
	if(openindex>=0) treeindex.close();
	if(keyequalityconds>=2) {goto exit_select;}
	//this else statement should run if a table has no B+Tree index
	while (rid < rf.endRid()) {
	// read the tuple
	if ((rc = rf.read(rid, key, value)) < 0) {
		fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
		goto exit_select;
	}
    
	// check the conditions on the tuple
		
	for (unsigned i = 0; i < cond.size(); i++) {
		// compute the difference between the tuple value and the condition value
		switch (cond[i].attr) {
			case 1:
			diff = key - atoi(cond[i].value);
			break;
			case 2:
			diff = strcmp(value.c_str(), cond[i].value);
			break;
		}

			  // skip the tuple if any condition is not met
		switch (cond[i].comp) {
		case SelCond::EQ:
			if (diff != 0) goto next_tuple;
		break;
		case SelCond::NE:
			if (diff == 0) goto next_tuple;
		break;
		case SelCond::GT:
			if (diff <= 0) goto next_tuple;
		break;
		case SelCond::LT:
			if (diff >= 0) goto next_tuple;
		break;
		case SelCond::GE:
			if (diff < 0) goto next_tuple;
		break;
		case SelCond::LE:
			if (diff > 0) goto next_tuple;
		break;
		}
	}

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;
    
	
    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

		// move to the next tuple
		next_tuple:
		++rid;
	} //end while
}
	end_select:
	// print matching tuple count if "select count(*)"
	if (attr == 4) {
		fprintf(stdout, "%d\n", count);
	}
	rc = 0;
	
	// close the table file and return
	exit_select:
	rf.close();
	return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{ 
	/* your code here */
  
	BTreeIndex indextree;
 
	RecordFile rf;  //where the table is stored/accessed
	RecordId rid;   //the id as a pointer to each pagefile and slot i dont know how this works very well
 
	RC rc; //i do not know what this variable is for
	int key;  //this is the first column in a table
	string value; //the second value in a table
  
	//variable to open the file to be read 
	fstream load_data;
	//open the table file to write to, if it doesn't exist then create one
	//prints error if there is something wrong


	//open the data to read it
	load_data.open(loadfile.c_str());
	//if data is not opened then output error 
	if (!load_data.is_open()) {
		cout << "could not open the file " + loadfile << "\n";
			return 0;
  
	}
  
    if(rc=rf.open(table+".tbl",'w')<0) {
		cout << "could not open the file for writing\n";
		return rc;
	} 
	
	if(index) {
		rc=indextree.open(table+".idx",'w');
		if(rc < 0) {return rc;}
	}
  
	//to store each line 
	string tvalues;
	//run while there is a line in the file to be read
	while(getline(load_data, tvalues)) {
		//parse line to separate each value
		if(rc=parseLoadLine(tvalues, key, value)<0) {
			cout << "error in parsing\n";
			return rc;
		}
		   //append values to the table file at the end
		if((rc=rf.append(key, value, rid))<0) {
			cout << "could not append\n";
			return rc;
		}
		
		if(index) {
			rc=indextree.insert(key, rid);
			if(rc<0) return rc;
		}
	  }
	if(index) {
		indextree.writedata();
		indextree.close();
	}
	//close RecordFile and file opened with fstream
	rf.close();
	load_data.close();
	return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
