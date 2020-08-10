#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include "Pipe.h"
#include "RelOp.h"
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"
#include <unistd.h>
#define PBSTR "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#define PBWIDTH 60
// SELECT l.l_orderkey, s.s_suppkey, o.o_orderkey FROM lineitem AS l, supplier AS s, orders AS o WHERE (l.l_suppkey = s.s_suppkey) AND (l.l_orderkey = o.o_orderkey)
// SELECT SUM DISTINCT (s.s_acctbal) FROM lineitem AS l, supplier AS s, orders AS o WHERE (l.l_suppkey = s.s_suppkey) AND (l.l_orderkey = o.o_orderkey) GROUP BY s.s_suppkey
// SELECT s.i FROM ssb AS s WHERE (s.i < 100)
// SELECT n.n_nationkey FROM nation AS n WHERE (n.n_nationkey < 100)

extern "C" {
	int yyparse (void);   // defined in y.tab.c
}

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int queryType;  // 1 for SELECT, 2 for CREATE, 3 for DROP,
					   // 4 for INSERT, 5 for SET, 6 for EXIT
// extern int outputType; // 0 for NONE, 1 for STDOUT, 2 for file output

extern char *outputVar;
	
extern char *tableName;
extern char *fileToInsert;

extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

char *catalog = "catalog";
char *stats = "Statistics.txt";

const int ncustomer = 150000;
const int nlineitem = 6001215;
const int nnation = 25;
const int norders = 1500000;
const int npart = 200000;
const int npartsupp = 800000;
const int nregion = 5;
const int nsupplier = 10000;

const int BUFFSIZE = 100;

static int pidBuffer = 0;

void printProgress (double percentage)
{
    int val = (int) (percentage * 100);
    int lpad = (int) (percentage * PBWIDTH);
    int rpad = PBWIDTH - lpad;
    printf ("\r%3d%% [%.*s%*s]", val, lpad, PBSTR, rpad, "");
    fflush (stdout);
    cout << endl;
}


int getPid () {
	
	return ++pidBuffer;
	
}

unordered_map<int, Pipe *> pipeMap;

enum NodeType {
	G, SF, SP, P, D, S, GB, J, W
};

class QueryNode {

public:
	
	int pid;  // Pipe ID
	
	NodeType t;
	Schema sch;  // Ouput Schema
	
	RelationalOp *relOp;
	
	QueryNode ();
	QueryNode (NodeType type) : t (type) {}
	
	~QueryNode () {}
	virtual void Print () {};
	virtual void Execute (unordered_map<int, Pipe *> &pipeMap) {};
	
	virtual void Wait () {
		
		relOp->WaitUntilDone ();
		
	}
	
};

class JoinNode : public QueryNode {

public:
	
	QueryNode *left;
	QueryNode *right;
	CNF cnf;
	Record literal;
	
	JoinNode () : QueryNode (J) {}
	~JoinNode () {
		
		if (left) delete left;
		if (right) delete right;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Join Operation" << endl;
		cout << "Input Pipe 1 ID : " << left->pid << endl;
		cout << "Input Pipe 2 ID : " << right->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema : " << endl;
		sch.Print ();
		cout << "Join CNF : " << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
		left->Print ();
		right->Print ();
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new Join ();
		((Join *)relOp)->Use_n_Pages(100);
		
		left->Execute (pipeMap);
		right->Execute (pipeMap);
		
		((Join *)relOp)->Run (*(pipeMap[left->pid]), *(pipeMap[right->pid]), *(pipeMap[pid]), cnf, literal);
		
		left->Wait ();
		right->Wait ();
		
//		j.WaitUntilDone ();
		
	}
	
};

class ProjectNode : public QueryNode {

public:
	
	int numIn;
	int numOut;
	int *attsToKeep;
	
	QueryNode *from;
	
	ProjectNode () : QueryNode (P) {}
	~ProjectNode () {
		
		if (attsToKeep) delete[] attsToKeep;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Project Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID " << pid << endl;
		cout << "Number Attrs Input : " << numIn << endl;
		cout << "Number Attrs Output : " << numOut << endl;
		cout << "Attrs To Keep :" << endl;
		for (int i = 0; i < numOut; i++) {
			
			cout << attsToKeep[i] << endl;
			
		}
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new Project ();
		
		from->Execute (pipeMap);
		
		((Project *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), attsToKeep, numIn, numOut);
		
		from->Wait ();
		
		// p.WaitUntilDone ();
		
	}
	
};

class SelectFileNode : public QueryNode {

public:
	
	bool opened;
	
	CNF cnf;
	DBFile file;
	Record literal;
	
	SelectFileNode () : QueryNode (SF) {}
	~SelectFileNode () {
		
		if (opened) {
			
			file.Close ();
			
		}
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Select File Operation" << endl;
		cout << "Output Pipe ID " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "Select CNF:" << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new SelectFile ();
		
		((SelectFile *)relOp)->Run (file, *(pipeMap[pid]), cnf, literal);
		
//		sf.WaitUntilDone ();
		
	}
	
};

class SelectPipeNode : public QueryNode {

public:
	
	CNF cnf;
	Record literal;
	QueryNode *from;
	
	SelectPipeNode () : QueryNode (SP) {}
	~SelectPipeNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Select Pipe Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "Select CNF:" << endl;
		cnf.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new SelectPipe ();
		
		from->Execute (pipeMap);
		
		((SelectPipe *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), cnf, literal);
		
		from->Wait ();
		
//		sp.WaitUntilDone ();
		
	}
	
};

class SumNode : public QueryNode {

public:
	
	Function compute;
	QueryNode *from;
	
	SumNode () : QueryNode (S) {}
	~SumNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Sum Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Function :" << endl;
		compute.Print ();
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new Sum ();
		
		from->Execute (pipeMap);
		
		((Sum *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), compute);
		
		from->Wait ();
		
//		s.WaitUntilDone ();
		
	}
	
};

class DistinctNode : public QueryNode {

public:
	
	QueryNode *from;
	
	DistinctNode () : QueryNode (D) {}
	~DistinctNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Duplication Elimation Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
	void Execute () {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new DuplicateRemoval ();
		
		from->Execute (pipeMap);
		
		((DuplicateRemoval *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), sch);
		
		from->Wait ();
		
//		dr.WaitUntilDone ();
		
	}
	
};

class GroupByNode : public QueryNode {

public:
	
	QueryNode *from;
	
	Function compute;
	OrderMaker group;
	
	GroupByNode () : QueryNode (GB) {}
	~GroupByNode () {
		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Group By Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Pipe ID : " << pid << endl;
		cout << "Output Schema : " << endl;
		sch.Print ();
		cout << "Function : " << endl;
		compute.Print ();
		cout << "OrderMaker : " << endl;
		group.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
//		cout << pid << endl;
		
		pipeMap[pid] = new Pipe (BUFFSIZE);
		
		relOp = new GroupBy ();
		
		from->Execute (pipeMap);
		
		((GroupBy *)relOp)->Run (*(pipeMap[from->pid]), *(pipeMap[pid]), group, compute);
		
		from->Wait ();
		
//		gb.WaitUntilDone ();
		
	}
	
};

class WriteOutNode : public QueryNode {

public:
	
	QueryNode *from;
	
	FILE *output;
	
	WriteOutNode () : QueryNode (W) {}
	~WriteOutNode () {
		
		if (output) delete output;		
		if (from) delete from;
		
	}
	
	void Print () {
		
		cout << "*********************" << endl;
		cout << "Write Out Operation" << endl;
		cout << "Input Pipe ID : " << from->pid << endl;
		cout << "Output Schema:" << endl;
		sch.Print ();
		cout << "*********************" << endl;
		
		from->Print ();
		
	}
	
	void Execute (unordered_map<int, Pipe *> &pipeMap) {
		
		relOp = new WriteOut ();
		
		from->Execute (pipeMap);
		
		((WriteOut *)relOp)->Run (*(pipeMap[from->pid]), output, sch);
		
		from->Wait ();
		
//		wo.WaitUntilDone ();
		
	}
	
};

typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;
/*
void initSchemaMap (SchemaMap &map) {
	
	map[string(region)] = Schema ("catalog", region);
	map[string(part)] = Schema ("catalog", part);
	map[string(partsupp)] = Schema ("catalog", partsupp);
	map[string(nation)] = Schema ("catalog", nation);
	map[string(customer)] = Schema ("catalog", customer);
	map[string(supplier)] = Schema ("catalog", supplier);
	map[string(lineitem)] = Schema ("catalog", lineitem);
	map[string(orders)] = Schema ("catalog", orders);
	
}
*/

void initSchemaMap (SchemaMap &map) {
	
	ifstream ifs (catalog);
	
	char str[100];
	
	while (!ifs.eof ()) {
		
		ifs.getline (str, 100);
		
		if (strcmp (str, "BEGIN") == 0) {
			
			ifs.getline (str, 100);
//			cout << str << endl;
			map[string(str)] = Schema (catalog, str);
			
		}
		
	}
	
	ifs.close ();
	
}

void initStatistics (Statistics &s) {
	
	s.AddRel (region, nregion);
	s.AddRel (nation, nnation);
	s.AddRel (part, npart);
	s.AddRel (supplier, nsupplier);
	s.AddRel (partsupp, npartsupp);
	s.AddRel (customer, ncustomer);
	s.AddRel (orders, norders);
	s.AddRel (lineitem, nlineitem);

	// region
	s.AddAtt (region, "r_regionkey", nregion);
	s.AddAtt (region, "r_name", nregion);
	s.AddAtt (region, "r_comment", nregion);
	
	// nation
	s.AddAtt (nation, "n_nationkey",  nnation);
	s.AddAtt (nation, "n_name", nnation);
	s.AddAtt (nation, "n_regionkey", nregion);
	s.AddAtt (nation, "n_comment", nnation);
	
	// part
	s.AddAtt (part, "p_partkey", npart);
	s.AddAtt (part, "p_name", npart);
	s.AddAtt (part, "p_mfgr", npart);
	s.AddAtt (part, "p_brand", npart);
	s.AddAtt (part, "p_type", npart);
	s.AddAtt (part, "p_size", npart);
	s.AddAtt (part, "p_container", npart);
	s.AddAtt (part, "p_retailprice", npart);
	s.AddAtt (part, "p_comment", npart);
	
	// supplier
	s.AddAtt (supplier, "s_suppkey", nsupplier);
	s.AddAtt (supplier, "s_name", nsupplier);
	s.AddAtt (supplier, "s_address", nsupplier);
	s.AddAtt (supplier, "s_nationkey", nnation);
	s.AddAtt (supplier, "s_phone", nsupplier);
	s.AddAtt (supplier, "s_acctbal", nsupplier);
	s.AddAtt (supplier, "s_comment", nsupplier);
	
	// partsupp
	s.AddAtt (partsupp, "ps_partkey", npart);
	s.AddAtt (partsupp, "ps_suppkey", nsupplier);
	s.AddAtt (partsupp, "ps_availqty", npartsupp);
	s.AddAtt (partsupp, "ps_supplycost", npartsupp);
	s.AddAtt (partsupp, "ps_comment", npartsupp);
	
	// customer
	s.AddAtt (customer, "c_custkey", ncustomer);
	s.AddAtt (customer, "c_name", ncustomer);
	s.AddAtt (customer, "c_address", ncustomer);
	s.AddAtt (customer, "c_nationkey", nnation);
	s.AddAtt (customer, "c_phone", ncustomer);
	s.AddAtt (customer, "c_acctbal", ncustomer);
	s.AddAtt (customer, "c_mktsegment", 5);
	s.AddAtt (customer, "c_comment", ncustomer);
	
	// orders
	s.AddAtt (orders, "o_orderkey", norders);
	s.AddAtt (orders, "o_custkey", ncustomer);
	s.AddAtt (orders, "o_orderstatus", 3);
	s.AddAtt (orders, "o_totalprice", norders);
	s.AddAtt (orders, "o_orderdate", norders);
	s.AddAtt (orders, "o_orderpriority", 5);
	s.AddAtt (orders, "o_clerk", norders);
	s.AddAtt (orders, "o_shippriority", 1);
	s.AddAtt (orders, "o_comment", norders);
	
	// lineitem
	s.AddAtt (lineitem, "l_orderkey", norders);
	s.AddAtt (lineitem, "l_partkey", npart);
	s.AddAtt (lineitem, "l_suppkey", nsupplier);
	s.AddAtt (lineitem, "l_linenumber", nlineitem);
	s.AddAtt (lineitem, "l_quantity", nlineitem);
	s.AddAtt (lineitem, "l_extendedprice", nlineitem);
	s.AddAtt (lineitem, "l_discount", nlineitem);
	s.AddAtt (lineitem, "l_tax", nlineitem);
	s.AddAtt (lineitem, "l_returnflag", 3);
	s.AddAtt (lineitem, "l_linestatus", 2);
	s.AddAtt (lineitem, "l_shipdate", nlineitem);
	s.AddAtt (lineitem, "l_commitdate", nlineitem);
	s.AddAtt (lineitem, "l_receiptdate", nlineitem);
	s.AddAtt (lineitem, "l_shipinstruct", nlineitem);
	s.AddAtt (lineitem, "l_shipmode", 7);
	s.AddAtt (lineitem, "l_comment", nlineitem);
	
}

void PrintParseTree (struct AndList *andPointer) {
  
	cout << "(";
  
	while (andPointer) {
	  
		struct OrList *orPointer = andPointer->left;
      
		while (orPointer) {
		  
			struct ComparisonOp *comPointer = orPointer->left;
			
			if (comPointer!=NULL) {
			
				struct Operand *pOperand = comPointer->left;
				
				if(pOperand!=NULL) {
					
					cout<<pOperand->value<<"";
					
				}
				
				switch(comPointer->code) {
					
					case LESS_THAN:
						cout<<" < "; break;
					case GREATER_THAN:
						cout<<" > "; break;
					case EQUALS:
						cout<<" = "; break;
					default:
						cout << " unknown code " << comPointer->code;
					
				}
				
				pOperand = comPointer->right;
				
				if(pOperand!=NULL) {
					
					cout<<pOperand->value<<"";
				}
				
			}
			
			if(orPointer->rightOr) {
				
				cout<<" OR ";
				
			}
			
			orPointer = orPointer->rightOr;
			
		}
		
		if(andPointer->rightAnd) {
			
			cout<<") AND (";
		}
		
		andPointer = andPointer->rightAnd;
		
	}
	
	cout << ")" << endl;
	
}

void PrintTablesAliases (TableList * tableList)	{
	
	while (tableList) {
		
		cout << "Table " << tableList->tableName;
		cout <<	" is aliased to " << tableList->aliasAs << endl;
		
		tableList = tableList->next;
		
	}
	
}

void CopyTablesNamesAndAliases (TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map)	{
	
	while (tableList) {
		
		s.CopyRel (tableList->tableName, tableList->aliasAs);
		
		map[tableList->aliasAs] = tableList->tableName;
		
		tableNames.push_back (tableList->aliasAs);
		
		tableList = tableList->next;
		
	}
	
}

void DeleteTableList (TableList *tableList) {
	
	if (tableList) {
		
		DeleteTableList (tableList->next);
		
		delete tableList->tableName;
		delete tableList->aliasAs;
		
		delete tableList;
		
	}
	
}

void PrintNameList (NameList *nameList) {
	
	while (nameList) {
		
		cout << nameList->name << endl;
		
		nameList = nameList->next;
	
	}
	
}

void CopyNameList (NameList *nameList, vector<string> &names) {
	
	while (nameList) {
		
		names.push_back (string (nameList->name));
		
		nameList = nameList->next;
	
	}
	
}

void DeleteNameList (NameList *nameList) {
	
	if (nameList) {
		
		DeleteNameList (nameList->next);
		
		delete[] nameList->name;
		
		delete nameList;
		
	}
	
}

void PrintFunction (FuncOperator *func) {
	
	if (func) {
		
		cout << "(";
		
		PrintFunction (func->leftOperator);
		
		cout << func->leftOperand->value << " ";
		if (func->code) {
			
			cout << " " << func->code << " ";
		
		}
		
		PrintFunction (func->right);
		
		cout << ")";
		
	}
	
}

void CopyAttrList (AttrList *attrList, vector<Attribute> &atts) {
	
	while (attrList) {
		
		Attribute att;
		
		att.name = attrList->name;
		
		switch (attrList->type) {
			
			case 0 : {
				
				att.myType = Int;
				
			}
			break;
			
			case 1 : {
				
				att.myType = Double;
				
			}
			break;
			
			case 2 : {
				
				att.myType = String;
				
			}
			break;
			
			// Should never come after here
			default : {}
			
		}
		
		atts.push_back (att);
		
		attrList = attrList->next;
		
	}
	
}

void DeleteFunction (FuncOperator *func) {
	
	if (func) {
		
		DeleteFunction (func->leftOperator);
		DeleteFunction (func->right);
		
		delete func->leftOperand->value;
		delete func->leftOperand;
		
		delete func;
		
	}
	
}

void DeleteAttrList (AttrList *attrList) {
	
	if (attrList) {
		
		DeleteAttrList (attrList->next);
		
		delete attrList->name;
		delete attrList;
		
	}
	
}

void cleanup () {
	
	DeleteNameList (groupingAtts);
	DeleteNameList (attsToSelect);
	DeleteNameList (attsToSort);
	DeleteAttrList (attsToCreate);
	DeleteFunction (finalFunction);
	DeleteTableList (tables);
	
	groupingAtts = NULL;
	attsToSelect = NULL;
	attsToSort = NULL;
	attsToCreate = NULL;
	finalFunction = NULL;
	tables = NULL;
	boolean = NULL;
	
	distinctAtts = 0;
	distinctFunc = 0;
	queryType = 0;
	
	pipeMap.clear ();
	
}

int main () {
	
	cleanup ();
	outputVar = "STDOUT";
	
	while (1) {
		
		cout << endl;
		cout << "SQL > ";
		yyparse ();
		
		if (queryType == 1) {
			
			// cout << "SELECT" << endl;
			/*
			cout << endl << "Print Boolean :" << endl;
			PrintParseTree (boolean);
			
			cout << endl << "Print TableList :" << endl;
			PrintTablesAliases (tables);
			
			cout << endl << "Print NameList groupingAtts :" << endl;
			PrintNameList (groupingAtts);
			
			cout << endl << "Print NameList attsToSelect:" << endl;
			PrintNameList (attsToSelect);
			
			cout << finalFunction << endl;
			cout << endl << "Print Function:" << endl;
			PrintFunction (finalFunction);
			
			cout << endl;
			*/
			vector<char *> tableNames;
			vector<char *> joinOrder;
			vector<char *> buffer (2);
			
			AliaseMap aliaseMap;
			SchemaMap schemaMap;
			Statistics s;
			
	//		cout << "!!!" << endl;
			initSchemaMap (schemaMap);
	// 		initStatistics (s);
			s.Read (stats);
	//		cout << "!!!" << endl;
			CopyTablesNamesAndAliases (tables, s, tableNames, aliaseMap);
			
	//		cout << tableNames.size () << endl;
			
	/*		for (auto iter = tableNames.begin (); iter != tableNames.end (); iter++) {
				
				cout << *iter << endl;
				
			}*/
			
			if (tableNames.size () > 2) {
				
				sort (tableNames.begin (), tableNames.end ());
				
				int minCost = INT_MAX, cost = 0;
				int counter = 1;
				
				do {
					
					Statistics temp (s);
					
					auto iter = tableNames.begin ();
					buffer[0] = *iter;
					
			//		cout << *iter << " ";
					iter++;
					
					while (iter != tableNames.end ()) {
						
			//			cout << *iter << " ";
						buffer[1] = *iter;
						
						cost += temp.Estimate (boolean, &buffer[0], 2);
						temp.Apply (boolean, &buffer[0], 2);
						
						if (cost <= 0 || cost > minCost) {
							
							break;
							
						}
						
						iter++;
					
					}
					
			//		cout << endl << cost << endl;
			//		cout << counter++ << endl << endl;
					
					if (cost > 0 && cost < minCost) {
						
						minCost = cost;
						joinOrder = tableNames;
						
					}
					
			//		char fileName[10];
			//		sprintf (fileName, "t%d.txt", counter - 1);
			//		temp.Write (fileName);
					
					cost = 0;
					
				} while (next_permutation (tableNames.begin (), tableNames.end ()));
			
			} else {
				
				joinOrder = tableNames;
				
			}
		//	cout << minCost << endl;
			
			QueryNode *root;
			
			auto iter = joinOrder.begin ();
			SelectFileNode *selectFileNode = new SelectFileNode ();
			
			char filepath[50];
		//	cout << aliaseMap[*iter] << endl;
			sprintf (filepath, "bin/%s.bin", aliaseMap[*iter].c_str ());
			
			selectFileNode->file.Open (filepath);
			selectFileNode->opened = true;
			selectFileNode->pid = getPid ();
			selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
			selectFileNode->sch.ResetSchema (*iter);
			
			selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
			
			iter++;
			if (iter == joinOrder.end ()) {
				
				root = selectFileNode;
				
			} else {
				
				JoinNode *joinNode = new JoinNode ();
				
				joinNode->pid = getPid ();
				joinNode->left = selectFileNode;
				
				selectFileNode = new SelectFileNode ();
				
				sprintf (filepath, "bin/%s.bin", aliaseMap[*iter].c_str ());
				selectFileNode->file.Open (filepath);
				selectFileNode->opened = true;
				selectFileNode->pid = getPid ();
				selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
				
				selectFileNode->sch.ResetSchema (*iter);
				selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
				
				joinNode->right = selectFileNode;
				joinNode->sch.GetSchemaForJoin (joinNode->left->sch, joinNode->right->sch);
				joinNode->cnf.GrowFromParseTreeForJoin(boolean, &(joinNode->left->sch), &(joinNode->right->sch), joinNode->literal);
				
				iter++;
				
				while (iter != joinOrder.end ()) {
					
					JoinNode *p = joinNode;
					
					selectFileNode = new SelectFileNode ();
					
					sprintf (filepath, "bin/%s.bin", (aliaseMap[*iter].c_str ()));
					selectFileNode->file.Open (filepath);
					selectFileNode->opened = true;
					selectFileNode->pid = getPid ();
					selectFileNode->sch = Schema (schemaMap[aliaseMap[*iter]]);
					selectFileNode->sch.ResetSchema (*iter);
					selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->sch), selectFileNode->literal);
					
					joinNode = new JoinNode ();
					
					joinNode->pid = getPid ();
					joinNode->left = p;
					joinNode->right = selectFileNode;
					
					joinNode->sch.GetSchemaForJoin (joinNode->left->sch, joinNode->right->sch);
					joinNode->cnf.GrowFromParseTreeForJoin(boolean, &(joinNode->left->sch), &(joinNode->right->sch), joinNode->literal);
					
					iter++;
					
				}
				
				root = joinNode;
				
			}
			
			QueryNode *temp = root;
			
			if (groupingAtts) {
				
				if (distinctFunc) {
					
					root = new DistinctNode ();
					
					root->pid = getPid ();
					root->sch = temp->sch;
					((DistinctNode *) root)->from = temp;
					
					temp = root;
					
				}
				
				root = new GroupByNode ();
				
				vector<string> groupAtts;
				CopyNameList (groupingAtts, groupAtts);
				
				root->pid = getPid ();
				((GroupByNode *) root)->compute.GrowFromParseTree (finalFunction, temp->sch);
				root->sch.GetSchemaForGroup (temp->sch, ((GroupByNode *) root)->compute.ReturnInt (), groupAtts);
				((GroupByNode *) root)->group.growFromParseTree (groupingAtts, &(root->sch));
				
				((GroupByNode *) root)->from = temp;
				
			} else if (finalFunction) {
				
				root = new SumNode ();
				
				root->pid = getPid ();
				((SumNode *) root)->compute.GrowFromParseTree (finalFunction, temp->sch);
				
				Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
				root->sch = Schema (NULL, 1, ((SumNode *) root)->compute.ReturnInt () ? atts[0] : atts[1]);
				
				((SumNode *) root)->from = temp;
				
			} else if (attsToSelect) {
				
				root = new ProjectNode ();
				
				vector<int> attsToKeep;
				vector<string> atts;
				CopyNameList (attsToSelect, atts);
				
				// cout << atts.size () << endl;
				
				root->pid = getPid ();
				root->sch.GetSchemaForProject(temp->sch, atts, attsToKeep);
				
				int *attstk = new int[attsToKeep.size ()];
				
				for (int i = 0; i < attsToKeep.size (); i++) {
					
					attstk[i] = attsToKeep[i];
					
				}
				
				((ProjectNode *) root)->attsToKeep = attstk;
				((ProjectNode *) root)->numIn = temp->sch.GetNumAtts ();
				((ProjectNode *) root)->numOut = atts.size ();
				
				((ProjectNode *) root)->from = temp;
				
			}
			
			if (strcmp (outputVar, "NONE") && strcmp (outputVar, "STDOUT")) {
				
				temp = new WriteOutNode ();
				
				temp->pid = root->pid;
				temp->sch = root->sch;
				((WriteOutNode *)temp)->output = fopen (outputVar, "w");
				((WriteOutNode *)temp)->from = root;
				
				root = temp;
				
			}
			
			if (strcmp (outputVar, "NONE") == 0) {
				
				cout << "Parse Tree : " << endl;
				root->Print ();
			
			} else {
				// root->Print();
				root->Execute (pipeMap);
				
			}
			
			int i = 0;
			
			if (strcmp (outputVar, "STDOUT") == 0) {
				
				Pipe *p = pipeMap[root->pid];
				Record rec;
				
				while (p->Remove (&rec)) {
					
					i++;
					cout << "-------------------"<<endl;
					cout << endl;
					cout << "-------------------"<<endl;
					root->sch.Print();
					rec.Print (&(root->sch));
					
				}
				
			}
			
			cout << i << " records found!" << endl;
			
		} else if (queryType == 2) {
	
//			cout << "CREATE" << endl;
			
//			cout << tableName << endl;
			
			if (attsToSort) {
				
				PrintNameList (attsToSort);
				
			}
			
			char fileName[100];
			char tpchName[100];
			
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (tpchName, "%s.tbl", tableName);
			
			DBFile file;
			
			vector<Attribute> attsCreate;
			
			CopyAttrList (attsToCreate, attsCreate);
			
			ofstream ofs(catalog, ifstream :: app);
			
			ofs << endl;
			ofs << "BEGIN" << endl;
			ofs << tableName << endl;
			ofs << tpchName <<endl;
			
			Statistics s;
			s.Read (stats);
//			s.Write (stats);
			s.AddRel (tableName, 0);
			
			for (auto iter = attsCreate.begin (); iter != attsCreate.end (); iter++) {
				
				s.AddAtt (tableName, iter->name, 0);
				
				ofs << iter->name << " ";
				
				cout << iter->myType << endl;
				switch (iter->myType) {
					
					case Int : {
						ofs << "Int" << endl;
					} break;
					
					case Double : {
						
						ofs << "Double" << endl;
						
					} break;
					
					case String : {
						
						ofs << "String" << endl;
						
					}
					// should never come here!
					default : {}
					
				}
				
			}
			
			ofs << "END" << endl;
			s.Write (stats);
			
			if (!attsToSort) {
				
				file.Create (fileName, heap, NULL);
			
			} else {
				
				Schema sch (catalog, tableName);
				
				OrderMaker order;
				
				order.growFromParseTree (attsToSort, &sch);
				
				SortInfo info;
				
				info.myOrder = &order;
				info.runLength = BUFFSIZE;
				
				file.Create (fileName, sorted, &info);
				
			}
			file.Close();
		} else if (queryType == 3) {
			
//			cout << "DROP" << endl;
			
//			cout << tableName << endl;
			
			char fileName[100];
			char metaName[100];
			char *tempFile = "tempfile.txt";
			
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (metaName, "%s.md", fileName);
			
			remove (fileName);
			remove (metaName);
			
			ifstream ifs (catalog);
			ofstream ofs (tempFile);
			
			while (!ifs.eof ()) {
				
				char line[100];
				
				ifs.getline (line, 100);
				
				if (strcmp (line, "BEGIN") == 0) {
					
					ifs.getline (line, 100);
					
					if (strcmp (line, tableName)) {
						
						ofs << endl;
						ofs << "BEGIN" << endl;
						ofs << line << endl;
						
						ifs.getline (line, 100);
						
						while (strcmp (line, "END")) {
							
							ofs << line << endl;
							ifs.getline (line, 100);
							
						}
						
						ofs << "END" << endl;
						
					}
					
				}
				
			}
			
			ifs.close ();
			ofs.close ();
			
			remove (catalog);
			rename (tempFile, catalog);
			remove (tempFile);
			
		} else if (queryType == 4) {
			
//			cout << "INSERT" << endl;
			
//			cout << fileToInsert << endl;
//			cout << tableName << endl;
			
			char fileName[100];
			char tpchName[100];
			
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (tpchName, "tpch/%s", fileToInsert);
			
//			cout << tpchName << endl;
			
			DBFile file;
			Schema sch (catalog, tableName);
			
			sch.Print ();
			
			if (file.Open (fileName)) {
				
				file.Load (sch, tpchName);
				
				file.Close ();
				
			}
			
		} else if (queryType == 5) {
			
//			cout << "SET" << endl;
			
//			cout << outputVar << endl;
			
		} else if (queryType == 6) {
			
//			cout << "EXIT" << endl << endl << endl;
//
			printProgress(0.25);
			sleep(1);
			printProgress(0.5);
			sleep(1);
			printProgress(0.75);
			sleep(1);
			printProgress(1);
			cout << endl;
			cout << "Database shut down successfully"<<endl;
			/*
			Statistics s;
			initStatistics (s);
			
			s.Write (stats);
			*/
			break;
			
		}
		
		cleanup ();
	
	}
	
	return 0;
	
}
