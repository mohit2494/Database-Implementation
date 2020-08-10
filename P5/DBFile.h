#ifndef DBFILE_H
#define DBFILE_H

#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include "Defs.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Pipe.h"

typedef enum {heap, sorted, tree,undefined} fType;
typedef enum {READ, WRITE,IDLE} BufferMode;
struct SortInfo {
	
	OrderMaker *myOrder;
	int runLength;
	
};

typedef struct {
    OrderMaker *o;
    int l;
} SortedStartUp;

// class to take care of meta data for each table
class Preference{
public:
    // Indicator for type of DBFile
    fType f_type;
    // Variable to store the state in which the page buffer is present;
    // Possible values : WRITE,READ and IDLE(default state when the file is just created )
    BufferMode pageBufferMode;

    // Variable to track Current Page from which we need to READ or WRITE to with the file. May have different value according to the mode.
    off_t currentPage;

    // Variable to trac the Current record during the READ and WRITE to the file.
    int currentRecordPosition;

    // Boolean to check if the page isFull or not.
    bool isPageFull;

    //  Variable to store the file path  to the preference file
    char * preferenceFilePath;

    // Boolean indicating if all the records in the page have been written in the file(disk) or not.
    bool allRecordsWritten;

    // Boolean indicating if the page needs to be rewritten or not.
    bool reWriteFlag;
    
    // OrderMaker as an Input For Sorted File.
    
    char orderMakerBits[sizeof(OrderMaker)];
    
    // OrderMaker as an Input For Sorted File.
    OrderMaker * orderMaker;
    
    // Run Length For Sorting
    int runLength;

};


class GenericDBFile{
protected:
    //  Used to read & write page to the disk.
    File myFile;
    //  Used as a buffer to read and write data.
    Page myPage;
    // Pointer to preference file
    Preference * myPreferencePtr;
    //  Used to keep track of the state.
    ComparisonEngine myCompEng;
public:
    GenericDBFile();
    int GetPageLocationToWrite();
    int GetPageLocationToRead(BufferMode mode);
    int GetPageLocationToReWrite();
    void Create (char * f_path,fType f_type, void *startup);
    int Open (char * f_path);
    
    //  virtual function
    virtual ~GenericDBFile();
    virtual void MoveFirst ();
    virtual void Add (Record &addme);
    virtual void Load (Schema &myschema, const char *loadpath);
    virtual int GetNext (Record &fetchme);
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    virtual int Close();
};

class HeapDBFile: public virtual GenericDBFile{
public:
    HeapDBFile(Preference * preference);
    ~HeapDBFile();
    void MoveFirst ();
    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();

};

class SortedDBFile: public  GenericDBFile{
    Pipe * inputPipePtr;
    Pipe * outputPipePtr;
    BigQ * bigQPtr;
    File newFile;
    Page outputBufferForNewFile;
    OrderMaker * queryOrderMaker;
    bool doBinarySearch;
    
public:
    SortedDBFile(Preference * preference);
    ~SortedDBFile();
    void MoveFirst ();
    void Add (Record &addme);
    void Load (Schema &myschema, const char *loadpath);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    int Close ();
    void MergeSortedInputWithFile();
    int BinarySearch(Record &fetchme, Record &literal,off_t low, off_t high);
};


// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:
//  Generic DBFile Pointer
    GenericDBFile * myFilePtr;
//  Used to keep track of the state.
    Preference myPreference;
public:
	// Constructor and Destructor
	DBFile ();
    ~DBFile ();
	
	// Function to load preference from the disk. This is needed to make read and writes persistent.
	// @input - the file path of the table to be created or opened. Each tbl file has a corresponding .pref
	void LoadPreference(char*f_path,fType f_type);

	// Function to dumpn the preference to the disk. This is needed to make read and writes persistent.
	void DumpPreference();

	//  Function to directly load data from the tbl files.
	/**
		The Load function bulk loads the DBFile instance from a text file, appending new data
		to it using the SuckNextRecord function from Record.h. The character string passed to Load is
		the name of the data file to bulk load.
	**/
	void Load (Schema &myschema, const char *loadpath);
	bool isFileOpen;
	
	/**
		Each DBFile instance has a “pointer” to the current record
		in the file. By default, this pointer is at the first record
		in the file, but it can move in response to record retrievals.
		The following function forces the pointer to correspond to the
		first record in the file.
	**/
	void MoveFirst ();

	/**
	 	In order to add records to the file,
		the function Add is used. In the case of
		the unordered heap file that you are implementing
		in this assignment, this function simply adds the
		new record to the end of the file
		Note that this function should actually consume addMe,
		so that after addMe has been put into the file, it cannot
		be used again. There are then two functions that allow
		for record retrieval from a DBFile instance; all are
		called GetNext.
	**/
	void Add (Record &addme);

	/**
		The first version of GetNext simply gets
		the next record from the file and returns it to the user,
		where “next” is defined to be relative to the current
		location of the pointer.After the function call returns,
		the pointer into the file is incremented, so a subsequent
		call to GetNext won’t return the same record twice. The
		return value is an integer whose value is zero if and
		only if there is not a valid record returned from the
		function call (which will be the case, for example, if
		the last record in the file has already been returned).
	**/
	int GetNext (Record &fetchme);

	/**
		The next version of GetNext also accepts a selection predicate
		(this is a conjunctive normal form expression). It returns the
		next record in the file that is accepted by the selection predicate.
		The literal record is used to check the selection predicate, and is
		created when the parse tree for the CNF is processed.
	**/
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	/**
		Next, there is a function that is used to actually create the file,
		called Create. The first parameter to this function is a text string
		that tells you where the binary data is physically to be located –
		you should store the actual database data using the File class from
		File.h. If you need to store any meta-data so that you can open it
		up again in the future (such as the type of the file when you re-open it)
		as I indicated above, you can store this in an associated text file – just
		take name and append some extension to it, such as .header, and write your
		meta-data to that file.

		The second parameter to the Create function tells you the type of the file.
		In DBFile.h, you should define an enumeration called myType with three possible
		values: heap, sorted, and tree. When the DBFile is created, one of these tree
		values is passed to Create to tell the file what type it will be. In this
		assignment, you obviously only have to deal with the heap case. Finally, the
		last parameter to Create is a dummy parameter that you won’t use for this assignment,
		but you will use for assignment two. The return value from Create is a 1 on success
		and a zero on failure.
	**/
    int Create (const char *fpath, fType file_type, void *startup);

	/**
		Next, we have Open. This function assumes that the DBFile already exists and has previously
		been created and then closed. The one parameter to this function is simply the physical location
		of the file. If your DBFile needs to know anything else about itself, it should have written this
		to an auxiliary text file that it will also open at startup. The return value is a 1 on success
		and a zero on failure.
	**/
    int Open (const char *fpath);
	/**
		Next, Close simply closes the file. The return value is a 1 on success and a zero on failure.
	**/
    int Close ();
};
#endif