#include "string"
#include "iostream"
#include "fstream"
#include <string.h>

/**
 *  class for defining utility functions to be used
 *  across the project. Import "utility.h" to use it.
**/
class Utilities {
    
    public:
    
        // check if file exists already at the given file path
        static int checkfileExist (const std::string& name) {
            if (FILE *file = fopen(name.c_str(), "r")) { fclose(file); return 1; }  
            else { return 0; }   
        }

        // get's a unique random int counter value
        static int getNextCounter () {
            ifstream ifile;
            ofstream ofile;
            int counter = 0;
            if (checkfileExist("counter.txt")){
                ifile.open("counter.txt",ios::in);
                ifile.read((char*)&counter,sizeof(int));
                ifile.close();
            }
            counter++;
            ofile.open("counter.txt",ios::out);
            ofile.write((char*)&counter,sizeof(int));
            ofile.close();
            return counter;
        }   

        // returns a char* new random file name with given extension
        static char* newRandomFileName(char* extension) {
            if ((extension == NULL) || (extension[0] == '\0')) { extension=""; }
            string str("temp_" + to_string(Utilities::getNextCounter()) + extension);
            char *cstr = new char[str.length() + 1];
            strcpy(cstr, str.c_str());
            return cstr;
        }
};