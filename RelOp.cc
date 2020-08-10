#include<sstream>
#include "RelOp.h"
#include "Utilities.h"

// vector for cleaning memory of a vector
#define CLEANUPVECTOR(v) \
	({ for(vector<Record *>::iterator it = v.begin(); it!=v.end(); it++) { \
		if(!*it) { delete *it; } }\
		v.clear();\
})

//------------------------------------------------------------------------------------------------
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {	
	// initialize
	this->inFile = &inFile; this->outPipe = &outPipe;
	this->selOp = &selOp; 	this->literal = &literal;
	// create thread
	pthread_create(&this->thread, NULL, caller, (void*)this);
}

// auxiliary functions
void SelectFile::WaitUntilDone () { pthread_join (thread, NULL); }
void SelectFile::Use_n_Pages (int runlen) { return; }
void* SelectFile::caller(void *args) { ((SelectFile*)args)->operation(); }

// function is called by the thread
void* SelectFile::operation() {
	int count=0;Record rec;ComparisonEngine cmp;
	inFile->MoveFirst();
	count=0;
	while(inFile->GetNext(rec)) {
		if (cmp.Compare(&rec, literal, selOp)) { outPipe->Insert(&rec);++count; }
	}
	cerr<< count <<" records read from SelectFile Relop"<<endl;
	outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	//initialize
	this->inPipe = &inPipe;	this->outPipe = &outPipe;
	this->selOp = &selOp;	this->literal = &literal;
	// create thread
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

// auxiliary functions
void SelectPipe::WaitUntilDone () { pthread_join (thread, NULL); }
void SelectPipe::Use_n_Pages (int n) { return; }
void* SelectPipe::caller(void *args) { ((SelectPipe*)args)->operation(); }

// function is called by the thread
void* SelectPipe::operation() {
	Record rec;
	ComparisonEngine cmp;
	while(inPipe->Remove(&rec)) {
		if (cmp.Compare(&rec, literal, selOp)) { outPipe->Insert(&rec); }
	}
	outPipe->ShutDown();
}
//------------------------------------------------------------------------------------------------
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	// initialize
	this->inPipe = &inPipe;	this->outPipe = &outPipe;
	this->keepMe = keepMe;	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	// create thread
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

// auxiliary functions
void Project::WaitUntilDone () { pthread_join (thread, NULL); }
void Project::Use_n_Pages (int n) { return; }
void* Project::caller(void *args) { ((Project*)args)->operation(); }

// function is called by the thread
void* Project::operation() {
	Record rec;
	while (inPipe->Remove(&rec)) { 
		rec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&rec);
	}
	outPipe->ShutDown();
}
//------------------------------------------------------------------------------------------------

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&thread, NULL, caller, (void *)this);

}
void Join::WaitUntilDone () { pthread_join (thread, NULL); }
void Join::Use_n_Pages (int n) { rl = n; }
void* Join::caller(void *args) { ((Join*)args)->operation(); }

// main join operation
void* Join::operation() {
	selOp->PrintWithSchema(new Schema("catalog", "region"), new Schema("catalog", "nation"), literal);
	// initialize
	Record lr, rr, m; 
	OrderMaker lom, rom;

	// getsortorders and create bigq
	selOp->GetSortOrders(lom,rom);
	
	// sorted-merge join or block-nested join
	if(lom.getNumAtts()>0&&rom.getNumAtts()>0) { 
		sortMergeJoin(lr,rr,m,lom,rom); 
	}
	else {
		blockNestedJoin(lr,rr,m,lom,rom); 
	}

	//shutdown pipe
	outPipe->ShutDown();
}

void Join::sortMergeJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom) {
	
	// intialize
	Pipe spl(500), spr(500); ComparisonEngine ce;
	BigQ lq(*inPipeL, spl, lom, rl); 
	BigQ rq(*inPipeR, spr, rom, rl);

	// sort and merge
	bool le=spl.Remove(&lr); bool re=spr.Remove(&rr);int c=0;int lc=1; int rc=1;
	while(le&&re) {
	
		int v = ce.Compare(&lr,&lom, &rr, &rom);
		if (v==-1)		{ le=spl.Remove(&lr);lc++;}
		else if	(v==1)	{ re=spr.Remove(&rr);rc++;}
		else {
			c++;
			vector<Record *> vl; vector <Record *> vr;
			Record *pushlr  = new Record();  pushlr->Consume(&lr);  vl.push_back(pushlr);
			Record *pushrr  = new Record();  pushrr->Consume(&rr);  vr.push_back(pushrr);

			le=spl.Remove(&lr);lc++;
			while(le && (!ce.Compare(&lr,pushlr,&lom))) {
				Record *tr = new Record(); tr->Consume(&lr); vl.push_back(tr);le=spl.Remove(&lr);lc++;
			}

			re=spr.Remove(&rr);rc++;
			while(re &&(!ce.Compare(&rr,pushrr,&rom))) {
				Record *tr = new Record(); tr->Consume(&rr); vr.push_back(tr);re=spr.Remove(&rr);rc++;
			}
			
			for(int i=0; i < vl.size(); i++){ for(int j=0; j< vr.size(); j++) { MergeRecord(vl[i], vr[j]); }}
			CLEANUPVECTOR(vl);CLEANUPVECTOR(vr);
		}
	}

	// empty pipes
	while(spl.Remove(&lr)); while(spr.Remove(&rr));
}

void Join::blockNestedJoin(Record lr,Record rr, Record m, OrderMaker &lom, OrderMaker &rom) {

	// initialize
	ComparisonEngine ce; Record r, *trl, *trr; int c=0, lc=0, rc=0; Page *lp, *rp; 

	// create dbfile; fill dbfile; move to first record
	DBFile dbf;
    char * fileName = Utilities::newRandomFileName(".bin");
    
    dbf.Create(fileName, heap, NULL);
	while(inPipeR->Remove(&r)) {
        ++c;
        dbf.Add(r);
    }
    dbf.Close();
    
    // file reopened and ready to read.
    dbf.Open(fileName);
    dbf.MoveFirst();

//    cout << "right file: " << c << endl;

	// if empty right pipe
	if (!c) {
        while(inPipeL->Remove(&lr));
        return;
    }
    
//  Variable For Block Merge
	lp = new Page();
    rp = new Page();
	trl = new Record();
    trr = new Record();
//    int pmc = 0;
	while(inPipeL->Remove(&lr)) {
		++lc;
		if (!lp->Append(&lr)) {
//			cout << "page number :" << ++lpid << " Page Count "<< lp->getNumRecs() << endl;
            
			Record * lpr; vector <Record *> lrvec;
            lpr = new Record();
            
            while(lp->GetFirst(lpr)){
				lrvec.push_back(lpr);
                lpr = new Record();
			}
            
            if (sizeof(lr.bits)) {
                lrvec.push_back(&lr);
            }
            
//            cout<<lrvec.size()<<" ex: "<<lrvec.size()*80<<endl;
            lrc += lrvec.size();
            
            Record * rpr; vector <Record *> rrvec;
            rpr = new Record();
            
            while (dbf.GetNext(rr)) {

                ++rrc;
                if(!rp->Append(&rr)) {
                    //                    cout << "page number :" << " Page Count "<< rp->getNumRecs() << endl;
                    while(rp->GetFirst(rpr)){
                        rrvec.push_back(rpr);
                        rpr = new Record();
                    }
                    if (sizeof(rr.bits)) {
                        rrvec.push_back(&rr);
                    }
                    //                    cout<<rrvec.size()<<endl;
                    for (int i=0; i < lrvec.size(); i++) {
                        for ( int j=0; j < rrvec.size();j++){
                            if (ce.Compare(lrvec[i], rrvec[j],literal, selOp)) {
                                ++mc;
                                MergeRecord(lrvec[i], rrvec[j]);
                            }
                        }
                    }
                    CLEANUPVECTOR(rrvec);
                }
            }
            
//            cout<<"Merges: "<<mc-pmc<<" total:"<<mc<<endl;
//            pmc=mc;
            dbf.MoveFirst();
            CLEANUPVECTOR(lrvec);
		}


	}
    if(lp->getNumRecs()){
        dbf.MoveFirst();
//        cout << "page number :" << ++lpid << " Page Count "<< lp->getNumRecs() << endl;
        Record * lpr; vector <Record *> lrvec;
        lpr = new Record();
        while(lp->GetFirst(lpr)){
            lrvec.push_back(lpr);
            lpr = new Record();
        }
//        cout<<lrvec.size()<<" ex : "<<lrvec.size()*80<<endl;
        lrc += lrvec.size();
        Record * rpr; vector <Record *> rrvec;
        rpr = new Record();
        while (dbf.GetNext(rr)) {
            ++rrc;
            if(!rp->Append(&rr)) {
                 while(rp->GetFirst(rpr)){
                     rrvec.push_back(rpr);
                     rpr = new Record();
                 }
                
                 if (sizeof(rr.bits)) {
                     rrvec.push_back(&rr);
                 }
                for (int i=0; i < lrvec.size(); i++) {
                    for ( int j=0; j < rrvec.size();j++){
                            if (ce.Compare(lrvec[i], rrvec[j],literal, selOp)) {
                                ++mc;
                                MergeRecord(lrvec[i], rrvec[j]);
                            }
                    }
                }
                CLEANUPVECTOR(rrvec);
                
            }
        }
//        cout<<"Merges: "<<mc-pmc<<" total:"<<mc<<endl;
//        pmc=mc;
        dbf.MoveFirst();
        CLEANUPVECTOR(lrvec);
        
    }
    
    dbf.Close();
    
    if(Utilities::checkfileExist(fileName)) {
        if( remove(fileName) != 0 )
        cerr<< "Error deleting file" ;
    }
    string s(fileName);
    string news = s.substr(0,s.find_last_of('.'))+".pref";
    char * finalString = new char[news.length()+1];
    strcpy(finalString, news.c_str());
    
    if(Utilities::checkfileExist(finalString)) {
        if( remove(finalString) != 0 )
        cerr<< "Error deleting file" ;
    }

//	cout << "left record count "  << lc << endl;
//	cout << "merge record count " << mc << endl;
//	cout << "----------------------------------------------" << endl;
//	cout << "processed record count "  << lrc << endl;
//	cout << "processed record count " << rrc << endl;

	// empty pipes
	while(inPipeL->Remove(&lr));
	while(inPipeR->Remove(&rr));
}

void Join::MergePages(vector <Record *> lrvec, Page *rp, OrderMaker &lom, OrderMaker &rom) {
	
	rrc += rp->getNumRecs();
	
	Record rpr;
	ComparisonEngine ce;

	for (int i=0; i < lrvec.size(); i++) {
		while(rp->GetFirst(&rpr)) {
			if (!ce.Compare(lrvec[i], &lom, &rpr, &rom)) {
				++mc;
				MergeRecord(lrvec[i], &rpr);
			}
		}
	
	}
}

// merge 2 records for join
void Join::MergeRecord(Record *lr, Record *rr) {
	
	int nal=lr->getNumAtts(), nar=rr->getNumAtts();
	int *atts = new int[nal+nar];
	for (int k=0;k<nal;k++) atts[k]=k;
	for (int k=0;k<nar;k++) atts[k+nal]=k;
	Record m;
	m.MergeRecords(lr, rr, nal, nar, atts, nal+nar, nal);
	// Schema nation("catalog","nation");
	// Schema region("catalog","region");
	// Schema joinSchema;
	// joinSchema.GetSchemaForJoin(region,nation);
	// m.Print(&joinSchema);
	outPipe->Insert(&m);
	delete atts;
	return;
}


//------------------------------------------------------------------------------------------------
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
	// initialize
	this->inPipe = &inPipe; this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	// create thread
	pthread_create(&this->thread,NULL, caller, (void*)this);
}

// auxiliary functions
void DuplicateRemoval::WaitUntilDone () { pthread_join (thread, NULL); }
void DuplicateRemoval::Use_n_Pages (int n) { this->rl = n; }
void* DuplicateRemoval::caller(void *args) { ((DuplicateRemoval*)args)->operation(); }

// function is called by the thread
void* DuplicateRemoval::operation() {

	// initialize
	OrderMaker om(mySchema); ComparisonEngine ce;
	Record pr, cr;Pipe *sp = new Pipe(100);
	BigQ sq(*inPipe, *sp, om, rl);
	
	// remove duplicates
	sp->Remove(&pr);
	while(sp->Remove(&cr)) {
		if(!ce.Compare(&pr, &cr, &om)) continue;
		outPipe->Insert(&pr);pr.Consume(&cr);
	}

	// insert and shut
	outPipe->Insert(&pr);outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

	// initialize
	this->inPipe = &inPipe; this->outPipe = &outPipe; 
	this->computeMe = &computeMe;

	// create thread
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

// auxiliary functions
void Sum::WaitUntilDone () { pthread_join (thread, NULL); }
void Sum::Use_n_Pages (int n) { return; }
void* Sum::caller(void *args) { ((Sum*)args)->operation(); }

// function is called by the thread
void* Sum::operation() {
	Record t; Record rec; Type rt;
	int si = 0; double sd = 0.0f;

	while(inPipe->Remove(&rec)) {
		Schema nation("catalog","nation");
		Schema region("catalog","region");
		Schema joinSchema;
		joinSchema.GetSchemaForJoin(nation,region);
		rec.Print(&joinSchema);
		int ti = 0; double td = 0.0f;
		rt = this->computeMe->Apply(rec, ti, td);
		rt == Int ? si += ti : sd += td;
	}

	char result[30];
	if (rt == Int) sprintf(result, "%d|", si); else sprintf(result, "%f|", sd);
	Type myType = (rt==Int)?Int:Double;
	Attribute sum = {(char *)"sum",myType};
	Schema os("something",1,&sum);
	t.ComposeRecord(&os,result);
	outPipe->Insert(&t);outPipe->ShutDown();
}

//------------------------------------------------------------------------------------------------
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	
	// initialize
	this->inPipe = &inPipe;	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts; this->computeMe = &computeMe;

	// create
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

// auxiliary functions
void GroupBy::WaitUntilDone () { pthread_join (thread, NULL); }
void GroupBy::Use_n_Pages (int n) { rl=n; }
void* GroupBy::caller(void *args) { ((GroupBy*)args)->operation(); }

// function is called by the thread
void* GroupBy::operation() {

	// Pipe sp(1000);BigQ bq(*inPipe,sp,*groupAtts,rl);
	// Record *gr = new Record(),*pr=new Record(),*cr=NULL;
	// int ir,si;double dr,sd;Type rt; ComparisonEngine ce;
	// bool grchng=false,pe = false;

	// // defensive check
	// if(!sp.Remove(pr)) { outPipe->ShutDown(); return 0; }
 
	// while(!pe) {
	// 	si =0;sd=0;ir=0;dr=0;
	// 	cr = new Record();
	// 	grchng = false;

	// 	while(!pe && !grchng) {
	// 		sp.Remove(cr);
	// 		// defensive check in case record is empty
	// 		if(cr->bits!=NULL) { 
	// 		if(ce.Compare(cr,pr,groupAtts)!=0){gr->Copy(pr);grchng=true;}}
	// 		else { pe = true; }
	// 		rt = computeMe->Apply(*pr,ir,dr);
	// 		if(rt==Int) { si += ir; } else if(rt==Double) { sd += dr; }
	// 		pr->Consume(cr);
	// 	}

	// 	Record *op = new Record();
		
	// 	if(rt==Double) {
	// 		Attribute a = {(char*)"sum", Double};Schema ss((char*)"somefile",1,&a);
	// 		char sstr[30];sprintf(sstr, "%f|", sd); op->ComposeRecord(&ss,sstr);
	// 	}

	// 	if (rt==Int) {
	// 		Attribute att = {(char*)"sum", Int};Schema ss((char*)"somefile",1,&att);
	// 		char sstr[30];sprintf(sstr, "%d|", si); op->ComposeRecord(&ss,sstr);
	// 	}

	// 	Record rr;
	// 	int nsatt = groupAtts->numAtts+1; int satt[nsatt]; satt[0]=0;
	// 	for(int i=1;i<nsatt;i++) { satt[i]=groupAtts->whichAtts[i-1]; }
	// 	rr.MergeRecords(op,gr,1,nsatt-1,satt,nsatt,1);
	// 	outPipe->Insert(&rr);
	// }
	// outPipe->ShutDown();

	Type t;
	Schema *sumSchema;
	Attribute att;
	stringstream ss;
	
	ComparisonEngine comp;
	
	Pipe sortPipe (500);
	
	Record *prev = new Record ();
	Record *curr = new Record ();
	Record *sum = new Record ();
	Record *newRec = new Record ();
	
	// cout << "group by started" << endl;
	
	BigQ bigq (*inPipe, sortPipe, *groupAtts, 100);
	
	int count = 0;
	
	int integerSum = 0, integerRec;
	double doubleSum = 0.0, doubleRec;
	
	char *sumStr = new char[100];
	
	int numAtts = groupAtts->numAtts;
	int *atts = groupAtts->whichAtts;
	int *attsToKeep = new int[numAtts + 1];
	
	attsToKeep[0] = 0;
	
	for (int i = 0; i < numAtts; i++) {
		
		attsToKeep[i + 1] = atts[i];
		
	}
	
	if (sortPipe.Remove (prev)) {
		
		t = computeMe->Apply (*prev, integerRec, doubleRec);
		
		if (t == Int) {
			
			integerSum += integerRec;
			// cout << integerRec << endl;
			
		} else {
			
			doubleSum += doubleRec;
			// cout << doubleRec << endl;
			
		}
		
	} else {
		
		cout << "No output from sortPipe!" << endl;
		
		outPipe->ShutDown();
		
		delete sumStr;
		delete sumSchema;
		delete prev;
		delete curr;
		delete sum;
		delete newRec;
		
		exit (-1);
		
	}
	
	att.name = "SUM";
	att.myType = t;
	
	sumSchema = new Schema (NULL, 1, &att);
	
	// cout << "doing group by" << endl;
	
	while (sortPipe.Remove (curr)) {
		
		if (comp.Compare (prev, curr, groupAtts) != 0) {
			// prev != curr
			
			if (t == Int) {
				
				sprintf (sumStr, "%d|", integerSum);
				
			} else {
				
				sprintf (sumStr, "%f|", doubleSum);
				
			}
			
			sum->ComposeRecord (sumSchema, sumStr);
			newRec->MergeRecords (sum, prev, 1, prev->GetLength (), attsToKeep, numAtts + 1, 1);
			
			// cout << "Inserted!" << endl;
			outPipe->Insert (newRec);
			
			count++;
			// cout << count << " group done!" << endl;
			
			
			doubleSum = 0.0;
			integerSum = 0;
			computeMe->Apply (*curr, integerRec, doubleRec);
			
			if (t == Int) {
			
				integerSum += integerRec;
				
			} else {
				
				doubleSum += doubleRec;
				
			}
			
			prev->Consume (curr);
			
			
		} else {
			// prev == curr
			
			computeMe->Apply (*curr, integerRec, doubleRec);
			
			if (t == Int) {
				
				integerSum += integerRec;
				
			} else {
				
				doubleSum += doubleRec;
				
			}
			
		}
		
	}
	
	if (t = Int) {
		
		sprintf (sumStr, "%d|", integerSum);
		
	} else {
		
		sprintf (sumStr, "%f|", doubleSum);
		
	}
	
	count++;
	sum->ComposeRecord (sumSchema, sumStr);
	newRec->MergeRecords (sum, prev, 1, prev->GetLength (), attsToKeep, numAtts + 1, 1);
	cout << endl;
	cout << "============="<< endl;
	sumSchema->Print();  cout <<endl;
	outPipe->Insert (newRec);
	cout << "=============="<< endl;
	cout << endl;
	// cout << count << " group done!" << endl;
	
	// cout << "Inserted " << count << " groups!" << endl;
	
	outPipe->ShutDown ();
	
	delete sumStr;
	delete sumSchema;
	delete prev;
	delete curr;
	delete sum;
	delete newRec;

}

//------------------------------------------------------------------------------------------------
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	this->inPipe = &inPipe;
	this->mySchema = &mySchema;
	this->outFile = outFile;
	pthread_create(&this->thread, NULL, caller, (void *)this);
}

// auxiliary
void WriteOut::WaitUntilDone () { pthread_join (thread, NULL);}
void WriteOut::Use_n_Pages (int n) { return; }
void* WriteOut::caller(void *args) { ((WriteOut*)args)->operation(); }

// function similar to record.Print()
void* WriteOut::operation() {
    Record t;
    long rc=0;
    // loop through all of the attributes
    int n = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();
    while (inPipe->Remove(&t) )
    {
        rc++;
        // loop through all of the attributes
        for (int i = 0; i < n; i++) {

            fprintf(outFile,"%s: ",atts[i].name);
            int pointer = ((int *) t.bits)[i + 1];
            
            if (atts[i].myType == Int)
            {
                int *mi = (int *) &(t.bits[pointer]);
                fprintf(outFile,"%d",*mi);

            }
            else if (atts[i].myType == Double)
            {
                double *md = (double *) &(t.bits[pointer]);
                fprintf(outFile,"%f",*md);
            }
            else if (atts[i].myType == String)
            {
                char *ms = (char *) &(t.bits[pointer]);
                fprintf(outFile,"%s",ms);
            }

            if (i != n - 1) {
                fprintf(outFile,"%c",'|');
            }
        }
        fprintf(outFile,"%c",'\n');
    }
    fclose(outFile);
    cout<<"\n Number of records written to output file : "<<rc<<"\n";
}

