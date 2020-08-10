#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

//-------------------------------------------------------------------------------------
class RelationalOp {

	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

//-------------------------------------------------------------------------------------
class SelectFile : public RelationalOp { 
	private:
	pthread_t thread;
	Record *literal;
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	// why static - https://stackoverflow.com/questions/1151582/pthread-function-from-a-class
	static void* caller(void*);
	void *operation();
};

//-------------------------------------------------------------------------------------
class SelectPipe : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
};

//-------------------------------------------------------------------------------------
class Project : public RelationalOp { 
	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
};

//-------------------------------------------------------------------------------------
class Join : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	int rl, mc=0, lrc=0, rrc=0;
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
	void MergePages(vector<Record*> lrvec, Page *rp, OrderMaker &lom, OrderMaker &rom);
	void MergeRecord(Record *lr, Record *rr);
	void sortMergeJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom);
	void blockNestedJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom);
};

//-------------------------------------------------------------------------------------
class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	int rl;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
};

//-------------------------------------------------------------------------------------
class Sum : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
};

//-------------------------------------------------------------------------------------
class GroupBy : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
	int rl;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
	void computeAndOutputSum(int intSum, double doubleSum, Function& func, Record& mergeWith,
	Record& mergeInto, OrderMaker &groupAtts, int* attsToKeep, Pipe& outPipe);
};

//-------------------------------------------------------------------------------------
class WriteOut : public RelationalOp {
	private:
	pthread_t thread;
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	static void* caller(void*);
	void *operation();
};
#endif
