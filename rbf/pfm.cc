#include "pfm.h"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <string.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
    // 
    
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    //This method creates an empty-paged file called fileName. The file should not already exist. This method should not create any pages in the file.
    // TODO
    // first examine whether this file already exist
    // create a file with the first 4096 is header page, header page should store at least three parameter
    // 1. int readPageCounter
    // 2. int writePageCounter
    // 3. int appendPageCounter

    FILE *pFile;

    pFile = fopen(fileName.c_str(),"r");
    if (pFile != NULL){ //exists
        fclose(pFile);
        return -1;
    }
    
    pFile = fopen(fileName.c_str(), "wb");
    // create header page with all 0
    void* data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);

    if (pFile != NULL){
        fwrite(data, sizeof(char), PAGE_SIZE, pFile);
        fclose(pFile);
        free(data);
        return 0;
        
    }else{
        free(data);
        return -1;
    }


    //return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    //This method destroys the paged file whose name is fileName. The file should already exist.
    // first check whether this file exist
    // Delete that file
    
    if (FILE * pfile = fopen(fileName.c_str(), "r")){// check whether the file exists
        fclose(pfile);

        // delete it
        if (remove(fileName.c_str()) == 0){
            return 0;
        }else{
            return -2;
        };


    }else{
        return -1;
    };



    //return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    /*
    This method opens the paged file whose name is fileName. 
    The file must already exist (and been created using the createFile method). 
    If the open method is successful, the fileHandle object whose address is passed in as a parameter now becomes a "handle" for the open file.
     This file handle is used to manipulate the pages of the file (see the FileHandle class description below). 
     It is an error if fileHandle is already a handle for some open file when it is passed to the openFile method. 
     It is not an error to open the same file more than once if desired, but this would be done by using a different fileHandle object each time. 
     Each call to the openFile method creates a new "instance" of FileHandle. 
     Warning: Opening a file more than once for data modification is not prevented by the PF component, but doing so is likely to corrupt the file structure and may crash the PF component. (You do not need to try and prevent this, as you can assume the layer above is "friendly" in that regard.) 
     Opening a file more than once for reading is no problem.
    */
   // 開一個file 
   // read in the file information e.g. readPageCount, writePageCount, ...

   //  check the file is already exist
    FILE *pFile;
    pFile = fopen(fileName.c_str(), "r+");
    
    if (pFile != NULL){

        // check if the fileHandle already handle some file
        if (fileHandle.pfile != NULL){
            return -1;
        }
        // point to end of the file
        fseek(pFile, 0, SEEK_END);
        fileHandle.pfile = pFile;
        fileHandle.readPC();
        
        return 0;

    }else{
        return -1;
    }


    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    // This method closes the open file instance referred to by fileHandle. 
    //  (The file should have been opened using the openFile method.) All of the file's pages are flushed to disk when the file is closed.
    // empty the variables
    //fseek(fileHandler.)
    
    fseek(fileHandle.pfile, 0, SEEK_SET);   
    fileHandle.writePC();

    fileHandle.readPageCounter = 0;
    fileHandle.writePageCounter = 0;
    fileHandle.appendPageCounter = 0;
    fclose(fileHandle.pfile);
    fileHandle.pfile = NULL;
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    pfile = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    // 判斷 pageNum 有沒有超過append page counter
    // readPage Counter += 1
    // use f.seek()
    // int fseek ( FILE * stream, long int offset, int origin );
    // if success return 0
    /*
    #include <stdio.h>
int main ()
{
  FILE * pFile;
  pFile = fopen ( "example.txt" , "wb" );
  fputs ( "This is an apple." , pFile );
  fseek ( pFile , 9 , SEEK_SET ); SEEK_SET	Beginning of file,  SEEK_CUR	Current position of the file pointer, SEEK_END	End of file *
  fputs ( " sam" , pFile );
  fclose ( pFile );
  return 0;
}
    */ 
   // check pageNum 有沒有超過append page counter
    if (pageNum >= appendPageCounter){
        return -1;
    }
    // get the offset of the pageNum
    int offset = (pageNum+1) * PAGE_SIZE;
    fseek(pfile, offset, SEEK_SET);
    fread(data, sizeof(char), PAGE_SIZE, pfile);
    readPageCounter++;
    return 0;


    //return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    // 判斷有沒有超過pageCount, 超過回傳-1
    // writePage Counter += 1
    if (pageNum >= appendPageCounter){
        return -1;
    }
    int offset = PAGE_SIZE * (pageNum+1);
    fseek(pfile, offset, SEEK_SET);
    fwrite(data, sizeof(char), PAGE_SIZE, pfile);
    writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    // appendPage Counter += 1
    // check the length of the data is 4096
    // write data on the end of file

    fseek(pfile, 0, SEEK_END);
    fwrite(data, sizeof(char), PAGE_SIZE, pfile);
    appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    // return append page number
    return appendPageCounter;
   
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    // paste the variables into the parameters.
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;


    //return -1;
}
void FileHandle::createEmptyPage(void *data){
    char tmp_page[PAGE_SIZE] = {0};
    int initial_free_space = PAGE_SIZE-sizeof(int)*2; // one for F(free space counter) one for N(record num)
    memcpy(tmp_page+PAGE_SIZE - sizeof(int), &initial_free_space, sizeof(int));
    memcpy((char *)data, tmp_page, PAGE_SIZE);

}
void FileHandle::writePC(){

    fseek(pfile, 0, SEEK_SET);
    fwrite(&readPageCounter, sizeof(int), 1, pfile);
    fwrite(&writePageCounter, sizeof(int), 1, pfile);
    fwrite(&appendPageCounter, sizeof(int), 1, pfile);
}

void FileHandle::readPC(){
    //cout << "still here"  << sizeof(unsigned) << "\n";
    fseek(pfile, 0, SEEK_SET);
    fread(&readPageCounter, sizeof(int), 1, pfile);
    // read w count
    fread(&writePageCounter, sizeof(int), 1, pfile);
    // read a count
    fread(&appendPageCounter, sizeof(int), 1, pfile);
}
