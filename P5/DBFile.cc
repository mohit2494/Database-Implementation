#include "DBFile.h"
#include "Utilities.h"

/*-----------------------------------------------------------------------------------*/
//                   GENERIC DBFILE CLASS FUNCTION DEFINATION
/*-----------------------------------------------------------------------------------*/
GenericDBFile::GenericDBFile(){
    
}

GenericDBFile::~GenericDBFile(){

}

int GenericDBFile::GetPageLocationToWrite() {
    int pageLocation = myFile.GetLength();
    return !pageLocation ? 0 : pageLocation-1;
}

int GenericDBFile::GetPageLocationToRead(BufferMode mode) {
    if (mode == WRITE){
        return myPreferencePtr->currentPage-2;
    }
    else if (mode == READ){
        return myPreferencePtr->currentPage;
    }
}

int GenericDBFile::GetPageLocationToReWrite(){
    int pageLocation = myFile.GetLength();
    return pageLocation == 2 ? 0 : pageLocation-2;

}

void GenericDBFile::Create(char * f_path,fType f_type, void *startup){
    // opening file with given file extension
    myFile.Open(0,(char *)f_path);
    if (startup!=NULL and f_type==sorted){
        myPreferencePtr->orderMaker = ((SortedStartUp *)startup)->o;
        myPreferencePtr->runLength  = ((SortedStartUp *)startup)->l;
    }
}

int GenericDBFile::Open(char * f_path){
    // opening file with given file extension
    myFile.Open(1,(char *)f_path);
    if(myFile.IsFileOpen()){
        // Load the last saved state from preference.
        if( myPreferencePtr->pageBufferMode == READ){
            myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
            Record myRecord;
            for (int i = 0 ; i < myPreferencePtr->currentRecordPosition; i++){
                myPage.GetFirst(&myRecord);
            }
            myPreferencePtr->currentPage++;
        }
        else if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->isPageFull){
            myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
        }
        return 1;
    }
    return 0;
}

void GenericDBFile::MoveFirst () {}

void GenericDBFile::Add(Record &addme){}

void GenericDBFile::Load(Schema &myschema, const char *loadpath){}

int GenericDBFile::GetNext(Record &fetchme){}

int GenericDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal){}

int GenericDBFile::Close(){}

/*-----------------------------------END--------------------------------------------*/


/*-----------------------------------------------------------------------------------*/
//                   HEAP DBFILE CLASS FUNCTION DEFINATION
/*-----------------------------------------------------------------------------------*/
HeapDBFile :: HeapDBFile(Preference * preference){
    myPreferencePtr = preference;
}

HeapDBFile::~HeapDBFile(){

}

void HeapDBFile::MoveFirst () {
    if (myFile.IsFileOpen()){
        if (myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!myPreferencePtr->allRecordsWritten){
                myFile.AddPage(&myPage,GetPageLocationToReWrite());
            }
        }
        myPage.EmptyItOut();
        myPreferencePtr->pageBufferMode = READ;
        myFile.MoveToFirst();
        myPreferencePtr->currentPage = 0;
        myPreferencePtr->currentRecordPosition = 0;
    }
}

void HeapDBFile :: Add (Record &rec) {

    if (!myFile.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

     // Flush the page data from which you are reading and load the last page to start appending records.
     if (myPreferencePtr->pageBufferMode == READ ) {
            if( myPage.getNumRecs() > 0){
                myPage.EmptyItOut();
            }
            // open page the last written page to start rewriting
            myFile.GetPage(&myPage,GetPageLocationToReWrite());
            myPreferencePtr->currentPage = GetPageLocationToReWrite();
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
            myPreferencePtr->reWriteFlag = true;
    }

    // set DBFile in write mode
    myPreferencePtr->pageBufferMode = WRITE;

    if(myPage.getNumRecs()>0 && myPreferencePtr->allRecordsWritten){
                    myPreferencePtr->reWriteFlag = true;
    }

    // add record to current page
    // check if the page is full
    if(!this->myPage.Append(&rec)) {

        //cout << "DBFile page full, writing to disk ...." << myFile.GetLength() << endl;

        // if page is full, then write page to disk. Check if the date needs to rewritten or not
        if (myPreferencePtr->reWriteFlag){
            this->myFile.AddPage(&this->myPage,GetPageLocationToReWrite());
            myPreferencePtr->reWriteFlag = false;
        }
        else{
            this->myFile.AddPage(&this->myPage,GetPageLocationToWrite());
        }

        // empty page
        this->myPage.EmptyItOut();

        // add again to page
        this->myPage.Append(&rec);
    }
    myPreferencePtr->allRecordsWritten=false;
}

void HeapDBFile :: Load (Schema &f_schema, const char *loadpath) {

    if (!myFile.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }

    Record temp;
    // Flush the page data from which you are reading and load the last page to start appending records.
    if (myPreferencePtr->pageBufferMode == READ ) {
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        // open page for write
        myFile.GetPage(&myPage,GetPageLocationToReWrite());
        myPreferencePtr->currentPage = GetPageLocationToReWrite();
        myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
        myPreferencePtr->reWriteFlag = true;
    }
    // set DBFile in WRITE Mode
    myPreferencePtr->pageBufferMode = WRITE;
    FILE *tableFile = fopen (loadpath, "r");
    // while there are records, keep adding them to the DBFile. Reuse Add function.
    while(temp.SuckNextRecord(&f_schema, tableFile)==1) {
        Add(temp);
    }

}

int HeapDBFile :: GetNext (Record &fetchme) {
    if (myFile.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if (myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!myPreferencePtr->allRecordsWritten){
                myFile.AddPage(&myPage,GetPageLocationToReWrite());
            }
            //  Only Write Records if new records were added.
            myPage.EmptyItOut();
            myPreferencePtr->currentPage = myFile.GetLength();
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
            return 0;
        }
        myPreferencePtr->pageBufferMode = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!myPage.GetFirst(&fetchme)) {
            // check if all records are read.
            if (myPreferencePtr->currentPage+1 >= myFile.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
                myPage.GetFirst(&fetchme);
                myPreferencePtr->currentPage++;
                myPreferencePtr->currentRecordPosition = 0;
            }
        }
        // increament counter for each read.
        myPreferencePtr->currentRecordPosition++;
        return 1;
    }
}

int HeapDBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
        // Flush the Page Buffer if the WRITE mode was active.
        if (myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            //  Only Write Records if new records were added.
            if(!myPreferencePtr->allRecordsWritten){
                myFile.AddPage(&myPage,GetPageLocationToReWrite());
            }
            myPage.EmptyItOut();
            myPreferencePtr->currentPage = myFile.GetLength();
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
            return 0;
        }
        bool readFlag ;
        bool compareFlag;
        // loop until all records are read or if some record maches the filter CNF
        do{
            readFlag = GetNext(fetchme);
            compareFlag = myCompEng.Compare (&fetchme, &literal, &cnf);
        }
        while(readFlag && !compareFlag);
        if(!readFlag){
            return 0;
        }
        return 1;

}

int HeapDBFile :: Close () {
    if (!myFile.IsFileOpen()) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    if(myPreferencePtr->pageBufferMode == WRITE && myPage.getNumRecs() > 0){
            if(!myPreferencePtr->allRecordsWritten){
                if (myPreferencePtr->reWriteFlag){
                    myFile.AddPage(&this->myPage,GetPageLocationToReWrite());
                    myPreferencePtr->reWriteFlag = false;
                }
                else{
                    myFile.AddPage(&this->myPage,GetPageLocationToWrite());
                }
            }
            myPreferencePtr->isPageFull = false;
            myPreferencePtr->currentPage = myFile.Close();
            myPreferencePtr->allRecordsWritten = true;
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
    }
    else{
        if(myPreferencePtr->pageBufferMode == READ){
            myPreferencePtr->currentPage--;
        }
        myFile.Close();
    }
    return 1;
}

/*-----------------------------------END--------------------------------------------*/


/*-----------------------------------------------------------------------------------*/
//                   SORTED DBFILE CLASS FUNCTION DEFINATION
/*-----------------------------------------------------------------------------------*/
SortedDBFile :: SortedDBFile(Preference * preference){
    myPreferencePtr = preference;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    queryOrderMaker= NULL;
    doBinarySearch=true;
}

SortedDBFile::~SortedDBFile(){

}

void SortedDBFile::MoveFirst () {
    if (myFile.IsFileOpen()){
         // Flush the Page Buffer if the WRITE mode was active.
        if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->allRecordsWritten){
                  MergeSortedInputWithFile();
        }
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        myPreferencePtr->pageBufferMode = READ;
        myFile.MoveToFirst();
        myPreferencePtr->currentPage = 0;
        myPreferencePtr->currentRecordPosition = 0;
        doBinarySearch=true;
    }
}

void SortedDBFile :: Add(Record &addme){
    if (!myFile.IsFileOpen()){
        cerr << "Trying to load a file which is not open!";
        exit(1);
    }
    
    
    // assign new pipe instance for input pipe if null
    if (inputPipePtr == NULL){
        inputPipePtr = new Pipe(10);
    }
    if(outputPipePtr == NULL){
        outputPipePtr = new Pipe(10);
    }
    if(bigQPtr == NULL){
        bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(myPreferencePtr->orderMaker), myPreferencePtr->runLength);
    }

    
     // Flush the page data from which you are reading and load the last page to start appending records.
     if (myPreferencePtr->pageBufferMode == READ ) {
        if( myPage.getNumRecs() > 0){
            myPage.EmptyItOut();
        }
        // assign new pipe instance for input pipe if null
        if (inputPipePtr == NULL){
            inputPipePtr = new Pipe(10);
        }
        if(outputPipePtr == NULL){
            outputPipePtr = new Pipe(10);
        }
        if(bigQPtr == NULL){
            bigQPtr =  new BigQ(*(inputPipePtr), *(outputPipePtr), *(myPreferencePtr->orderMaker), myPreferencePtr->runLength);
        }
    }
    
    // set DBFile in write mode
    myPreferencePtr->pageBufferMode = WRITE;
    
    // add record to input pipe
    inputPipePtr->Insert(&addme);
    
    // set allrecords written as false
    myPreferencePtr->allRecordsWritten=false;
}

void SortedDBFile :: Load(Schema &myschema, const char *loadpath){
    if (!myFile.IsFileOpen()){
          cerr << "Trying to load a file which is not open!";
          exit(1);
      }

      Record temp;
      // Flush the page data from which you are reading and load the last page to start appending records.
       if (myPreferencePtr->pageBufferMode == READ ) {
              if( myPage.getNumRecs() > 0){
                  myPage.EmptyItOut();
              }
              // assign new pipe instance for input pipe if null
               if (inputPipePtr == NULL){
                    inputPipePtr = new Pipe(10);
               }
      }
    
      // set DBFile in WRITE Mode
      myPreferencePtr->pageBufferMode = WRITE;
      FILE *tableFile = fopen (loadpath, "r");
      // while there are records, keep adding them to the DBFile. Reuse Add function.
      while(temp.SuckNextRecord(&myschema, tableFile)==1) {
          Add(temp);
      }
}

int SortedDBFile :: GetNext(Record &fetchme){
    if (myFile.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->allRecordsWritten){
                   MergeSortedInputWithFile();
        }
        myPreferencePtr->pageBufferMode = READ;
        // loop till the page is empty and if empty load the next page to read
        if (!myPage.GetFirst(&fetchme)) {
            // check if all records are read.
            if (myPreferencePtr->currentPage+1 >= myFile.GetLength()){
               return 0;
            }
            else{
                // load new page and get its first record.
                myFile.GetPage(&myPage,GetPageLocationToRead(myPreferencePtr->pageBufferMode));
                myPage.GetFirst(&fetchme);
                myPreferencePtr->currentPage++;
                myPreferencePtr->currentRecordPosition = 0;
            }
        }
        // increament counter for each read.
        myPreferencePtr->currentRecordPosition++;
        return 1;
    }
}

int SortedDBFile :: GetNext(Record &fetchme, CNF &cnf, Record &literal){
    
    if (myFile.IsFileOpen()){
        // Flush the Page Buffer if the WRITE mode was active.
        if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->allRecordsWritten){
            MergeSortedInputWithFile();
        }
        
        // set page mode to READ
        myPreferencePtr->pageBufferMode = READ;


        if(doBinarySearch){
            // read the current page for simplicity.
            off_t startPage = !myPreferencePtr->currentPage?0:myPreferencePtr->currentPage-1;
            // loop till the current page is fully read and checked record by record. Once the current page changes or the file ends break from the loop
            while(GetNext(fetchme)){
                if (startPage != myPreferencePtr->currentPage-1){
                    break;
                }
                if(myCompEng.Compare(&fetchme, &literal, &cnf)){
                    return 1;
                }
            }
            // has the value of the next page{
            if(myPreferencePtr->currentPage == myFile.GetLength()-1){
                startPage = myPreferencePtr->currentPage;
            }
            else{
                startPage= (myPreferencePtr->currentPage-1);
            }
            
            // fetch the queryOrderMaker using the cnf given and the sort ordermaker stored in the preference.
            queryOrderMaker= cnf.GetQueryOrderMaker(*myPreferencePtr->orderMaker);

            // if the queryOrderMaker is available do a binary search to find the first matching record which is equal to literal using queryOrderMaker.
            if(queryOrderMaker!=NULL){
                off_t endPage = (myFile.GetLength())-2;
                int retstatus;
                if(endPage > 0){
                    retstatus = BinarySearch(fetchme,literal,startPage,endPage);
                }
                else{
                    retstatus = GetNext(fetchme);
                }
                if(retstatus){
                    doBinarySearch = false;
                    if (retstatus == 1){
                        if(myCompEng.Compare(&fetchme, &literal, &cnf))
                            return 1;
                    }

                }
            }
            else
            {
                doBinarySearch = false;
            }

            //If the first record of binary search gives a match we cannot be sure that the previous page is not a match. So go Back from the returned page from the binary search untill you find a page which doesnot match.
            // remove page and make space for the correct page to be read in.
            myPage.EmptyItOut();
            int previousPage = myPreferencePtr->currentPage-1;
            Page prevPage;
            bool brkflag=false;
            while(previousPage>=startPage && queryOrderMaker!=NULL)
            {
                myFile.GetPage(&prevPage,previousPage);
                prevPage.GetFirst(&fetchme);
                
                if(myCompEng.Compare(&literal, queryOrderMaker, &fetchme, myPreferencePtr->orderMaker)==0)
                {
                    previousPage--;
                }
                else
                {
                    while(prevPage.GetFirst(&fetchme))
                    {
                        if(myCompEng.Compare (&fetchme, &literal, &cnf))
                        {
                            char *bits = new (std::nothrow) char[PAGE_SIZE];
                            myPage.EmptyItOut();
                            prevPage.ToBinary (bits);
                            myPage.FromBinary(bits);
                            myPreferencePtr->currentPage = previousPage+1;
                            return 1;
                        }
                    }
                    brkflag=true;
                }
                if(brkflag)
                {
                    myPreferencePtr->currentPage = previousPage+1;
                    break;
                }
            }
            //spcal case
            if(brkflag==true)
            {
            }

        }

        // Doing a linear Scan from the Record from where the seach stopped.
        while (GetNext(fetchme))
        {
            // if queryOrderMaker exists and the literal is smaller than the fetched record stop the seach as all other records are greter incase of the search.
            if (queryOrderMaker!=NULL && myCompEng.Compare(&literal,queryOrderMaker,&fetchme, myPreferencePtr->orderMaker) < 0){
                return 0;
            }
            // if the record passes the CNF compare return.
            if(myCompEng.Compare (&fetchme, &literal, &cnf))
            {
                return 1;
            }
        }
        // if nothing matches and we read all the file.
        return 0;
            
    }
}

int SortedDBFile :: Close(){
    if (!myFile.IsFileOpen()) {
        cout << "trying to close a file which is not open!"<<endl;
        return 0;
    }
    
    if(myPreferencePtr->pageBufferMode == WRITE && !myPreferencePtr->allRecordsWritten){
            MergeSortedInputWithFile();
            myPreferencePtr->isPageFull = false;
            myPreferencePtr->currentPage = myFile.Close();
            myPreferencePtr->allRecordsWritten = true;
            myPreferencePtr->currentRecordPosition = myPage.getNumRecs();
    }
    else{
        if(myPreferencePtr->pageBufferMode == READ){
            myPreferencePtr->currentPage--;
        }
        myFile.Close();
    }
    return 1;
    
}

void SortedDBFile::MergeSortedInputWithFile(){
    // setup for new file
    string fileName(myPreferencePtr->preferenceFilePath);
    string newFileName = fileName.substr(0,fileName.find_last_of('.'))+".nbin";
    char* new_f_path = new char[newFileName.length()+1];
    strcpy(new_f_path, newFileName.c_str());
    
    // open new file in which the data will be written.
    newFile.Open(0,new_f_path);
    Page page;
    int newFilePageCounter = 0;
    
    // shut down input pipe;
    inputPipePtr->ShutDown();
    
    // flush the read buffer
    if(myPage.getNumRecs() > 0){
        myPage.EmptyItOut();
    }
    
    // page counts;
    int totalPages = myFile.GetLength()-1;
    int currentPage = 0;
    // set flags to initiate merge
    bool fileReadFlag = true;
    bool outputPipeReadFlag = true;
    
    
    // merge data from file and output pipe
    bool getNextFileRecord = true;
    bool getNextOutputPipeRecord = true;
    Record * fileRecordptr = NULL;
    Record * outputPipeRecordPtr = NULL;
    
    // loop until data is there in either the pipe or file.
    while(fileReadFlag || outputPipeReadFlag){
        
        if (getNextOutputPipeRecord){
            outputPipeRecordPtr = new Record();
            getNextOutputPipeRecord = false;
            if(!outputPipePtr->Remove(outputPipeRecordPtr)){
                outputPipeReadFlag= false;
            }
        }
        
        if (getNextFileRecord){
            fileRecordptr=new Record();
            getNextFileRecord = false;
            if(!myPage.GetFirst(fileRecordptr)){
                if(currentPage<totalPages){
                    myFile.GetPage(&myPage,currentPage);
                    myPage.GetFirst(fileRecordptr);
                    currentPage++;
                }
                else{
                    fileReadFlag= false;
                }
            }
        }
        
        // select record to be written
        Record * writeRecordPtr;
        bool consumeFlag = false;
        if(fileReadFlag and outputPipeReadFlag){
            if (myCompEng.Compare(fileRecordptr,outputPipeRecordPtr,myPreferencePtr->orderMaker)<=0){
                writeRecordPtr = fileRecordptr;
                fileRecordptr=NULL;
                consumeFlag=true;
                getNextFileRecord=true;
            }
            else{
                writeRecordPtr = outputPipeRecordPtr;
                outputPipeRecordPtr=NULL;
                consumeFlag=true;
                getNextOutputPipeRecord=true;
            }
        }
        else if (fileReadFlag){
            writeRecordPtr = fileRecordptr;
            fileRecordptr=NULL;
            consumeFlag=true;
            getNextFileRecord=true;

        }
        else if (outputPipeReadFlag){
            writeRecordPtr = outputPipeRecordPtr;
            outputPipeRecordPtr=NULL;
            consumeFlag=true;
            getNextOutputPipeRecord=true;
        }
        
        
        if(consumeFlag && !page.Append(writeRecordPtr)) {
            // Add the page to new File
            newFile.AddPage(&page,newFilePageCounter);
            newFilePageCounter++;
            
            // empty page if not consumed
            if(page.getNumRecs() > 0){
                page.EmptyItOut();
            }
            // add again to page
            page.Append(writeRecordPtr);
        }
        if(writeRecordPtr!=NULL){
            delete writeRecordPtr;
        }
    }
    
    // write the last page which might not be full.
    if (page.getNumRecs() > 0){
        newFile.AddPage(&page,newFilePageCounter);
    }
    
    // set that all records in the input pipe buffer are written
    myPreferencePtr->allRecordsWritten=true;
    
    // clean up the input pipe, output pipe and bigQ after use.
    delete inputPipePtr;
    delete outputPipePtr;
    delete bigQPtr;
    inputPipePtr = NULL;
    outputPipePtr = NULL;
    bigQPtr = NULL;
    
    // close the files and swap the files
    myFile.Close();
    newFile.Close();
    
    string oldFileName = fileName.substr(0,fileName.find_last_of('.'))+".bin";
    char* old_f_path = new char[oldFileName.length()+1];
    strcpy(old_f_path, oldFileName.c_str());
    
    // deleting old file
    if(Utilities::checkfileExist(old_f_path)) {
        if( remove(old_f_path) != 0 )
        cerr<< "Error deleting file" ;
    }
    // renaming new file to old file.
    rename(new_f_path,old_f_path);
    
    //opening the new file.
    myFile.Open(1,old_f_path);
}

int SortedDBFile::BinarySearch(Record &fetchme, Record &literal,off_t low, off_t high){
    off_t mid = low + (high - low)/2;
    while(low <= high){
           if(mid != 0)
                  myFile.GetPage(&myPage,mid);
           else
                  myFile.GetPage(&myPage,0);
           myPage.GetFirst(&fetchme);
           int comparisonResult = myCompEng.Compare(&literal, queryOrderMaker, &fetchme, myPreferencePtr->orderMaker);

           if (comparisonResult == 0 ){
               myPreferencePtr->currentPage = mid+1;
               return 2;
           }
           else if (comparisonResult<0){
               high = mid - 1;
           }
           if (comparisonResult > 0){
                while(myPage.GetFirst(&fetchme)){
                    int comparisonResultInsidePage = myCompEng.Compare(&literal, queryOrderMaker,&fetchme, myPreferencePtr->orderMaker);
                    if(comparisonResultInsidePage<0){
                        return 0;
                    }
                    else if(comparisonResultInsidePage == 0 ){
                        myPreferencePtr->currentPage = mid+1;
                        return 1;
                    }
                }
                low = mid+1;
           }
          // calculate new mid using new low and high
           mid = low+(high - low)/2;
       }
       return 0;
}

/*-----------------------------------END--------------------------------------------*/



/*-----------------------------------------------------------------------------------*/
//                   DBFILE CLASS FUNCTION DEFINATION
/*-----------------------------------------------------------------------------------*/

DBFile::DBFile () {
    myFilePtr = NULL;
}

DBFile::~DBFile () {
    delete myFilePtr;
    myFilePtr = NULL;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if (Utilities::checkfileExist(f_path)) {
        cout << "file you are about to create already exists!"<<endl;
        return 0;
    }
    // changing .bin extension to .pref for storing preferences.
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    // loading preferences
    LoadPreference(finalString,f_type);
    
    // check if the file type is correct
    if (f_type == heap){
        myFilePtr = new HeapDBFile(&myPreference);
        myFilePtr->Create((char *)f_path,f_type,startup);
        return 1;
    }
    else if(f_type == sorted){
        myFilePtr = new SortedDBFile(&myPreference);
        myFilePtr->Create((char *)f_path,f_type,startup);
        return 1;
    }
    return 0;
}

int DBFile::Open (const char *f_path) {

    if (!Utilities::checkfileExist(f_path)) {
        cout << "Trying to open a file which is not created yet!"<<endl;
        return 0;
    }

    // changing .bin extension to .pref for storing preferences.
    string s(f_path);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    
    char* finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());

    // loading preferences
    LoadPreference(finalString,undefined);
    
    
    // check if the file type is correct
    if (myPreference.f_type == heap){
        myFilePtr = new HeapDBFile(&myPreference);
    }
    else if(myPreference.f_type == sorted){
        myFilePtr = new SortedDBFile(&myPreference);
    }
    // opening file using given path
    return myFilePtr->Open((char *)f_path);
}

void DBFile::Add (Record &rec) {
    if (myFilePtr!=NULL){
        myFilePtr->Add(rec);
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    if (myFilePtr!=NULL){
           myFilePtr->Load(f_schema,loadpath);
    }
}

void DBFile::MoveFirst () {
    if (myFilePtr!=NULL){
           myFilePtr->MoveFirst();
    }
}

int DBFile::Close () {
    if (myFilePtr!=NULL){
        if (myFilePtr->Close()){
            DumpPreference();
            return 1;

        }
        else{
            return 0;
        };
    }
}

int DBFile::GetNext (Record &fetchme) {
    if (myFilePtr != NULL){
        return myFilePtr->GetNext(fetchme);
    }
    return 0;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if (myFilePtr != NULL){
        return myFilePtr->GetNext(fetchme,cnf,literal);
    }
    return 0;
}

void DBFile::LoadPreference(char * newFilePath,fType f_type) {
    ifstream file;
    if (Utilities::checkfileExist(newFilePath)) {
        file.open(newFilePath,ios::in);
        if(!file){
            cerr<<"Error in opening file..";
            exit(1);
        }
        file.read((char*)&myPreference,sizeof(Preference));
        myPreference.preferenceFilePath = (char*)malloc(strlen(newFilePath) + 1);
        strcpy(myPreference.preferenceFilePath,newFilePath);
        if (myPreference.f_type == sorted){

        myPreference.orderMaker = new OrderMaker();
        file.read((char*)myPreference.orderMaker,sizeof(OrderMaker));
        }
    }
    else {
        myPreference.f_type = f_type;
        myPreference.preferenceFilePath = (char*) malloc(strlen(newFilePath) + 1);
        strcpy(myPreference.preferenceFilePath,newFilePath);
        myPreference.currentPage = 0;
        myPreference.currentRecordPosition = 0;
        myPreference.isPageFull = false;
        myPreference.pageBufferMode = IDLE;
        myPreference.reWriteFlag= false;
        myPreference.allRecordsWritten = true;
        myPreference.orderMaker = NULL;
        myPreference.runLength = 0;
    }
}

void DBFile::DumpPreference(){
    ofstream file;
    file.open(myPreference.preferenceFilePath,ios::out);
    if(!file) {
        cerr<<"Error in opening file for writing.."<<endl;
        exit(1);
    }
    file.write((char*)&myPreference,sizeof(Preference));
    if (myPreference.f_type == sorted){
        file.write((char*)myPreference.orderMaker,sizeof(OrderMaker));
    }
    file.close();
}

/*-----------------------------------END--------------------------------------------*/
