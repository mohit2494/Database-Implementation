#ifndef RELATIONSTATS_
#define RELATIONSTATS_
#include <map>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

using namespace std;
class RelStats{
    
    long noOfTuples;
    string groupName;
    int groupSize;
    map<string,int> attributeMap;
public:
    RelStats(int numTuples, string relName){
        this->noOfTuples = numTuples;
        this->groupName = relName;
        this->groupSize = 1;
    }
    ~RelStats(){
        attributeMap.clear();
    }
    
    RelStats(RelStats &copyMe){
        this->noOfTuples = copyMe.GetNofTuples();
        map<string,int> * ptr = copyMe.GetRelationAttributes();
        map<string,int>::iterator itr;
        
        for( itr = ptr->begin(); itr!=ptr->end(); itr++)
        {
            attributeMap[itr->first] = itr->second;
        }
        this->groupSize = copyMe.GetGroupSize();
        this->groupName = copyMe.GetGroupName();
    }
        
//   Getter Functions
    string GetGroupName()
    {
        return groupName;
    }
    int GetGroupSize()
    {
        return groupSize;
    }
    map<string,int> * GetRelationAttributes()
    {
        return &attributeMap;
    }
    long GetNofTuples()
    {
        return noOfTuples;
    }

// Setter Functions
    void UpdateNoOfTuples(int numTuples)
    {
        this->noOfTuples = numTuples;
    }

    void UpdateAttributes(string attName,int numDistincts)
    {   if (numDistincts == -1){
            numDistincts = noOfTuples;
        }
        this->attributeMap[attName] = numDistincts;
    }
    
    void UpdateGroup(string groupName,int groupCount)
    {
        this->groupName = groupName;
        this->groupSize = groupCount;
    }
    
};  

#endif
