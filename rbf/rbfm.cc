#include "rbfm.h"

#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <math.h>
#include <algorithm> 
#include <string.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {

    return pfm->createFile(fileName);

    //return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
    //return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}



void printBytes(void* data, int length){
    for(int i=0; i < length; i ++){
        unsigned char tmp = *((unsigned char *)(data) + i);
        cout << int(tmp) << "|";
    }
    cout << endl;
}
//template<typename Type, int Size>
void printArray(int array[], int size){
    for(int i=0; i < size; i++){
        cout << array[i] << "|";
    }
    cout << endl;
}
template<typename Type>
void printVector(vector<Type> const &v) {
    int i;
    for(i = 0; i<v.size(); i++)
        cout << v[i] << "|";
    cout << endl;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    vector<Attribute>::const_iterator it;
    void* record = malloc(PAGE_SIZE);
    int recordSize;

    // interpret the record format
    // get the actual record format in pages
    //cout << recordDescriptor.size() << recordDescriptor[0].name << recordDescriptor[1].name << recordDescriptor[2].name << recordDescriptor[3].name << endl;
    interpretRecord(recordDescriptor, data, record, &recordSize);  
    //cout << recordSize << endl;

    if (recordSize > PAGE_SIZE - sizeof(int)*2){
        //cout << "record size exceed page size : "<< recordSize << endl;
        return -1;
    }

    //pageToWrite = findFreePages(fileHandle, recordSize + sizeof(int)*2);
    //fileHandle.readPage(pageToWrite, page);
    int next_record_pos = 0;

    RID insertPlace = findFreePages_Slot(fileHandle, recordSize + sizeof(int)*2, next_record_pos);
    // insert the record into specific page and slot by RID
    insertByRID(fileHandle, insertPlace, next_record_pos, recordSize, record);

    rid.pageNum = insertPlace.pageNum;
    rid.slotNum = insertPlace.slotNum+1;
    free(record);
    
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    if (isDeleted(fileHandle, rid)){
        return -1;
    }

    if(isTomb(fileHandle, rid)){
        //cout << "is TOmbb!!!! " << endl;
        RID true_rid = getRidFromTombStone(fileHandle, rid);
        return readRecord(fileHandle, recordDescriptor, true_rid, data);
    }

    // get the page num and slot num
    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;

    // read out the pages
    void* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);

    // check whether the record is tombstone



    fileHandle.readPage(pageNum, page);

    // get where the slot is on page by slot number
    int slot_pos = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*(slotNum)*2;
    //cout << "slot pos: " << slot_pos << endl;

    // get the record position from slot 
    int record_pos;
    memcpy(&record_pos, (char *)page + slot_pos , sizeof(int));
    //cout << "record_pos:" << record_pos << endl;

    if(record_pos == -1){// the slot has been deleted
        return -1;
    }

    // get the record length 
    int recordSize;
    memcpy(&recordSize, (char *)page + slot_pos + sizeof(int) , sizeof(int));
    
    // get free space
    int free_space;
    memcpy( &free_space, (char *)page + PAGE_SIZE - sizeof(int) ,sizeof(int));
    
    // get record
    void* record = malloc(recordSize);
    memset(record, 0, recordSize);
    memcpy((char *)record, (char *)page + record_pos, recordSize);

    // get the field size
    int field_size;
    memcpy( &field_size, (char *)page + record_pos, sizeof(int));


    //cout << "Reading Record, page num: " << pageNum << " slotNum: " << slotNum << " get page count: " << fileHandle.getNumberOfPages() << " slot pos: " << slot_pos << " record pos: " << record_pos << endl;
    //cout << "free space: " << free_space << " "  << "record offset: " << record_pos <<  " record size: " << recordSize << endl;

    
    // store the offset of the field in the array
    short* offset_arr = new short[field_size];
    short* length_arr = new short[field_size];

    for (int i =0; i < field_size; i++){
        memcpy(&offset_arr[i], (char *)page+record_pos + 4 + i*sizeof(short)*2 ,sizeof(short));
    }
    
    for (int i =0; i < field_size; i++){
        memcpy(&length_arr[i], (char *)page+record_pos + 4 + 2 + i*sizeof(short)*2 ,sizeof(short));
    }


    // construct null field s
    int nullIndicatorLength = ceil(field_size/8.0);
    char *nullField = new char[nullIndicatorLength](); // this will set the array to 0

    //constructNullField(nullField, nullIndicatorLength, offset_arr);

    // get the null field
    for (int i =0; i < nullIndicatorLength; i++){
        for(int j=0; j < 8; j++){
            // if offset arr is 0, this field is null
            if(i*8+j < field_size && offset_arr[i*8+j] == 0){
                nullField[i] = nullField[i] | (1 << (8-j-1));
            }
        }
    }

    int buffer_offset = 0;
    vector<Attribute>::const_iterator it;
    

    // paste the null field to data;

    for (int i =0 ; i < nullIndicatorLength; i++){
        memcpy((char*)data + buffer_offset, nullField+i, sizeof(char));
        buffer_offset += 1;
    }
    
    // paste the record to data;
    int i;
    for(i=0, it=recordDescriptor.begin(); it!=recordDescriptor.end(); it++, i++){

        char* paste_pos = (char *)record + offset_arr[i];
        if(offset_arr[i] == 0){
            continue;
        }
        if(it->type == 0){
            memcpy((char *)data + buffer_offset, paste_pos, sizeof(int));
            buffer_offset += sizeof(int);
        }
        else if (it->type == 1){
            memcpy((char *)data+ buffer_offset, paste_pos, sizeof(float));
            buffer_offset += sizeof(float);

        }
        else if (it->type == 2){
            // for varchar we need to copy name length
            int nameLength = 0;
            nameLength = int(length_arr[i]);
     
            memcpy((char *)data + buffer_offset, &nameLength, sizeof(int));
            buffer_offset += sizeof(int);
            // copy data
            memcpy((char *)data + buffer_offset, paste_pos, nameLength);
            buffer_offset += nameLength;
        }
    }

    delete [] nullField;
    delete [] offset_arr;
    delete [] length_arr;
    free(record);
    free(page);

    return 0;

}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {


    void* record = malloc(PAGE_SIZE);
    int recordSize;
    //cout << "yee" << endl;
    // interpret the record format
    // get the actual record format in pages
    interpretRecord(recordDescriptor, data, record, &recordSize);

    //cout << "record: "<< endl;
    //printBytes(record, recordSize);

    // dir_offset is position of directory 
    short dir_offset = 4; // for first is number of field

    
    short next_offset = 0;
    short length = 0;
    int i;
    vector<Attribute>::const_iterator it;
    for(i = 0, it=recordDescriptor.begin(); it != recordDescriptor.end(); it++, i++){

        string name = it->name;

        short offset = 0;
 
        bool isnull = false;
        memcpy(&offset, (char *)record + dir_offset, sizeof(short));
        dir_offset += sizeof(short);
        memcpy(&length, (char *)record+ dir_offset, sizeof(short));
        dir_offset += sizeof(short);

        // if offset == 0, means this field is null
        if (offset == 0){
            isnull = true;
        }

        if (isnull == true){
            // null value
            cout << it->name << "\t" << "NULL" <<endl;
        }
    
        else{
            if(it->type == 0){
                
                int value;
                memcpy(&value, (char *)record + offset, sizeof(int));
                cout << name << "\t" << value << endl;
            }
            else if (it->type==1){
                
                float value;
                memcpy(&value, (char *)record + offset, sizeof(float));
                
                cout << name << "\t" << value << endl;
            }
            else if (it->type==2){
                char* value = new char[length]();
                
                memcpy(value, (char *)record + offset, length );
                cout << name << "\t" ;
                // should print by for loop,
                // otherwise the string might not be right, since we didn't append \n
                for(int ii = 0; ii < length; ii++){
                    cout << value[ii];
                }
                cout << endl;

            }
        }
        
    }  
    free(record);


    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){

    // get the page num and slot num
    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;

    // read out the pages
    void* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    fileHandle.readPage(pageNum, page);

    // get where the slot is on page by slot number
    int slot_pos = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*(slotNum)*2;

    // get the record position from slot 
    int record_pos;
    memcpy(&record_pos, (char *)page + slot_pos , sizeof(int)); 

    int record_length;
    memcpy(&record_length, (char *)page + slot_pos + sizeof(int), sizeof(int)); 


    if (record_pos == -1){ // if the record offset is -1, it means it is already deleted
        free(page);
        return -1;
    }


    if (isTomb(fileHandle, rid)){
        //cout <<"IS TOMB" << endl;
        RID true_rid = getRidFromTombStone(fileHandle, rid);
        deleteRecord(fileHandle, recordDescriptor, true_rid);
        //void* page = malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum, page);
        organizePage(page, rid, NULL, 0, -sizeof(int)*3);
        fileHandle.writePage(rid.pageNum, page);
        free(page);
        return 0;
    }

    // delete page is update the record with an empty record
    //void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    organizePage(page, rid, NULL, 0, -record_length);
    fileHandle.writePage(rid.pageNum, page);
    free(page);
    return 0;

    

}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

    // interpret the record to the format we should place in recordPagedFile
    void* update_record = malloc(PAGE_SIZE);
    int update_recordSize;
    // interpret the record format
    // get the actual record format in pages
    interpretRecord(recordDescriptor, data, update_record, &update_recordSize);  

    // get the origin record in the page
    void* origin_data = malloc(PAGE_SIZE);
    void* origin_record = malloc(PAGE_SIZE);
    int origin_recordSize;
    RID true_rid = rid;
    bool istomb = false;

    if (isTomb(fileHandle, rid)){
        //cout << "is tomb!!!!! " << endl;
        true_rid = getRidFromTombStone(fileHandle, rid);
        istomb = true;
    }

    readRecord(fileHandle, recordDescriptor, true_rid, origin_data);
    interpretRecord(recordDescriptor, origin_data, origin_record, &origin_recordSize);

    void* page = malloc(PAGE_SIZE);

    int increase_space = update_recordSize - origin_recordSize;
    if ( increase_space > getPageFreeSpace(fileHandle, true_rid.pageNum)){
        if (istomb){
            // if the rid point to a tombstone and we should move the record to new page again
            // we should first delete the record at the page
            // then find the new page to accommodate it and renew the origin tombstone
            deleteRecord(fileHandle, recordDescriptor, true_rid);
        }
        // move the record to other page
        
        // insert the record based on the new rid
        RID new_rid;
        insertRecord(fileHandle, recordDescriptor, data, new_rid);
        // leave tombstone on the original record
        
        // contruct tombstone
        void* tombstone = malloc(12); // one for field length, one for pageNum, one for slotNum
        memset(tombstone, 0, 12);
        
        int field_length = -1;
        int pageNum = new_rid.pageNum;
        int slotNum = new_rid.slotNum;
        memcpy((char *)tombstone, &field_length, sizeof(int));
        memcpy((char *)tombstone + sizeof(int), &pageNum, sizeof(int));
        memcpy((char *)tombstone + sizeof(int)*2, &slotNum, sizeof(int));


        // the origin page record shrink, so we need to modified the record
        fileHandle.readPage(rid.pageNum, page);
        if (istomb){
            organizePage(page, rid, tombstone, sizeof(int)*3, 0);
        }else{
            organizePage(page, rid, tombstone, sizeof(int)*3, sizeof(int)*3 - origin_recordSize);
        }
        fileHandle.writePage(rid.pageNum, page);

    }
    else{
        //cout << "don't need to change page.!!!!" << endl;
        // record modified, scan all the slot that offset > update record offset,
        // add their offset by increase space
        //cout << "true rid: " << true_rid.pageNum << " " << true_rid.slotNum << endl;
        //cout << "increase space: " << increase_space << endl;
        fileHandle.readPage(true_rid.pageNum, page);
        organizePage(page, true_rid, update_record, update_recordSize, increase_space);
        fileHandle.writePage(true_rid.pageNum, page);
        
    }
    free(origin_data);
    free(origin_record);
    free(page);
    free(update_record);

    return 0;




}
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    void* data_form = malloc(PAGE_SIZE);
    void* record = malloc(PAGE_SIZE);
    int recordSize;
    RC rc;
    // readRecord(fileHandle, recordDescriptor, rid, record);
    rc = readRecord(fileHandle, recordDescriptor, rid, data_form);
    if(rc == -1) {
        free(data_form);
        free(record);
        return rc;
    }
    interpretRecord(recordDescriptor, data_form, record, &recordSize);  
    

    int target_field = -1;
    int type = 0;
    // scan the recordDescriptor to get the index of attribute name
    
    for(int i = 0; i < recordDescriptor.size(); i ++){
        if (recordDescriptor[i].name == attributeName){
            target_field = i;
            type = recordDescriptor[i].type;
            break;
        }
    }
    short offset;
    memcpy(&offset, (char *)record + sizeof(int) + sizeof(short)*2*(target_field), sizeof(short));
    short length;
    memcpy(&length, (char *)record + sizeof(int) + sizeof(short)*2*(target_field) + sizeof(short), sizeof(short));
    //cout << offset << " " << length << endl;
    length = int(length);
    
    // construct null field
    char nullField = ((offset == 0) << 7); // if the value is null, offset will be 0
    int data_offset = 0;
    memcpy((char* )data, &nullField, sizeof(char));
    data_offset += sizeof(char);

    if (type == 2){
        memcpy((char *)data + data_offset, &length, sizeof(int));
        data_offset += sizeof(int);
    }

    memcpy((char *)data+data_offset, (char *)record + offset, length);
    data_offset += length;

    free(record);
    free(data_form);
    return 0;
}




void RecordBasedFileManager::interpretRecord(const vector<Attribute> &recordDescriptor, const void* data, void* record, int* recordSize){
    /*
    This function interpret TA's record format into the format that we should store in pages.
    TA foramt:  [null field ] .. [data field], [data field]...
    */
    // get the data size by recordDescriptor first
    int field_size = recordDescriptor.size();


    // Null field length
    int nullIndicatorLength = ceil((double)field_size/8.0);
    // get null indicator
    void* nullField = malloc(nullIndicatorLength);
    memcpy(nullField, data, nullIndicatorLength);



    // extract null field to vector
    vector<bool> isNull;
    getNullVector(nullIndicatorLength, nullField, isNull);
    
    
    // buffer is a place to store actual data (not include record directory)
    void* buffer = malloc(PAGE_SIZE);
    
    // offset is a where record actually append.
    int offset = 0;
    offset += sizeof(int); // for initial length of record
    // for one offset per field
    for(int i = 0; i < recordDescriptor.size(); i++){
        offset += sizeof(short); // for offset
        offset += sizeof(short); // for length
    }

   
    // create offset arrays to store each offset (on record).
    vector<short> offset_array;
    vector<short> length_array;

    
    int data_offset = 0; // the offset on void* data
    int data_size = 0 ; // the offset on buffer
    data_offset += nullIndicatorLength;

    int i;
    vector<Attribute>::const_iterator it;
    for(i = 0, it=recordDescriptor.begin(); it!=recordDescriptor.end(); it++, i++){
        if (!isNull[i]){
            if (it->type == 2){ // varchar type
                // get the nameLength of varchar first
                int nameLength;
                memcpy(&nameLength, (char *)data + data_offset, sizeof(int));

                data_offset += sizeof(int);
                // store the offset on record
                offset_array.push_back(offset);
                
                // copy data from void* data to buffer
                memcpy((char *)buffer + data_size, (char *)data+data_offset, nameLength);
                length_array.push_back(nameLength);
                offset += nameLength;
                data_offset += nameLength;
                data_size += nameLength;
            }
            else if (it->type == 0){ // int type
                offset_array.push_back(offset);
                memcpy((char *)buffer + data_size, (char *)data+data_offset, sizeof(int));
                length_array.push_back(sizeof(int));
                data_size += sizeof(int);
                data_offset += sizeof(int);
                offset += sizeof(int);
            }
            else if (it->type == 1){ // float type
                offset_array.push_back(offset);
                memcpy((char *)buffer + data_size, (char *)data+data_offset, sizeof(float));
                length_array.push_back(sizeof(float));
                data_size += sizeof(float);
                data_offset += sizeof(float);
                offset += sizeof(float);
            }
        }
        else{
        
            offset_array.push_back(0);
            length_array.push_back(0);
        }
    }
    

    int tmp_offset = 0;
    // paste the length of record at start
    memcpy((char *)record, &field_size, sizeof(int));
    tmp_offset += sizeof(int);

    // paste the offset directory to record
    for(int i = 0; i < offset_array.size(); i++){
        memcpy((char *)record + tmp_offset, &offset_array[i], sizeof(short));
        tmp_offset += sizeof(short);
        memcpy((char *)record + tmp_offset, &length_array[i], sizeof(short));
        tmp_offset += sizeof(short);
    }
    // paste on the buffer
    memcpy((char* )record + tmp_offset, (char *)buffer, data_size);

    *recordSize = offset;

    free(buffer);
    free(nullField);
    
}
/*
int RecordBasedFileManager::findFreePages(FileHandle &fileHandle, int needSpace){
    
    First check the last page has enough free space. If not, find the free space from 0 to last-1,
    If no page is larger enough, append a new page.
    return (int) the page number should write.
    
    void *page = malloc(PAGE_SIZE);
    int lastPageNum = fileHandle.getNumberOfPages()-1;
    
    if (lastPageNum != -1){
        fileHandle.readPage(lastPageNum, page);
         int free_space;
    
        memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    
        if(free_space >= needSpace){
            return lastPageNum;
        }
    }
   
    // if the last page doesn't have enough space. 
    // check from 0 until find page that can store.
    // if no page is enough, append a new page
    int pageCount = fileHandle.getNumberOfPages();
    
    for(int i = 0; i < pageCount-1; i++){// pageCount - 1 to avoid last page, which we check it already
        fileHandle.readPage(i, page);
        int free_space;
        memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
        if(free_space >= needSpace){
            free(page);
            return i;
        }
    }
    fileHandle.createEmptyPage(page);
    fileHandle.appendPage(page);
    free(page);
    return fileHandle.getNumberOfPages()-1;
}*/

bool RecordBasedFileManager::findSlotToInsert(FileHandle &fileHandle, int pgNum, void* page, int& next_record_pos, int& slotNum, int needSpace){
    fileHandle.readPage(pgNum, page);

    int free_space;
    memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    int record_num;
    memcpy(&record_num, (char *)page + PAGE_SIZE - sizeof(int)*2, sizeof(int));

    if(free_space >= needSpace){
        // traverse all the slot(including NULL-data slots)
        int i = 0;
        int slot_offset;
        slotNum = -1; // init
        while(i < record_num){
            memcpy(&slot_offset, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i+1), sizeof(int));
            // if the slot is a NULL-data slot,
            if(slot_offset == -1){
            	slotNum = i;
            	break;
            }
            ++i;
        }
        if(slotNum == -1) slotNum = record_num;
        next_record_pos = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*(record_num)*2 - free_space;
        return true;
    }
    return false;
}

RID RecordBasedFileManager::findFreePages_Slot(FileHandle &fileHandle, int needSpace, int& next_record_pos){
    RID insertPlace;
    void *page = malloc(PAGE_SIZE);
    // record slot number to return, initiate to -1
    int slotNum;
    // last_page
    int lastPageNum = fileHandle.getNumberOfPages()-1;
    //cout << "lastPageNum is " << lastPageNum << endl;
    if (lastPageNum != -1){
        // create a function: take page_idx as input and output the slotNum, next_record_pos
        if(findSlotToInsert(fileHandle, lastPageNum, page, next_record_pos, slotNum, needSpace)){
            insertPlace.pageNum = lastPageNum;
            insertPlace.slotNum = slotNum;
            free(page);
            //cout << "Last Page" << endl;
            return insertPlace;
        }
    }
    // 0~ last_page-1
    int pageCount = fileHandle.getNumberOfPages();
    for(int i = 0; i < pageCount-1; i++){// pageCount - 1 to avoid last page, which we check it already
        if(findSlotToInsert(fileHandle, i, page, next_record_pos, slotNum, needSpace)){
            insertPlace.pageNum = i;
            insertPlace.slotNum = slotNum;
            free(page);
            //cout << "MIDDLE PAGE" << endl;
            return insertPlace;
        }
    }
    // append new page
    fileHandle.createEmptyPage(page);
    fileHandle.appendPage(page);
    insertPlace.pageNum = fileHandle.getNumberOfPages()-1;
    insertPlace.slotNum = 0;
    next_record_pos = 0;
    free(page);
    //cout << insertPlace.pageNum << ' ' << insertPlace.slotNum << endl;
    //cout << "Append Page" << endl;
    return insertPlace;
}

void RecordBasedFileManager::getNullVector(int nullIndicatorLength, void* nullField, vector<bool> &isNull){
    // use bit manipulation to extract the null value 
    for(int i = 0; i < nullIndicatorLength; i++ ){
        unsigned char tmp = *((unsigned char *)nullField+i);
        for(int j = 7; j >= 0 ; j--){
            if ((tmp & (1 << j)) ){
                isNull.push_back(true);
            }
        
            else{
                isNull.push_back(false);
            }
        }
    }
    
}

int RecordBasedFileManager::getPageFreeSpace(FileHandle &fileHandle, int pageNum){
    void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, page);

    int free_space;
    memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    free(page);
    return free_space;

}

void RecordBasedFileManager::organizePage(void* page, RID rid, void* update_record, int update_recordSize ,int increase_space){

    int free_space;
    memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    
    int record_num;
    memcpy(&record_num, (char *)page + PAGE_SIZE - sizeof(int)*2, sizeof(int));

    int update_slot_offset;
    memcpy(&update_slot_offset, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum), sizeof(int));

    void* temp_page = malloc(PAGE_SIZE);
    memcpy((char *)temp_page, (char *)page, PAGE_SIZE);

    //cout << "origin page: ";
    //printBytes(temp_page, PAGE_SIZE);
    // traverse all the slot, if the slot offset > update slot offset, we should adjust the offset
    //cout << "increase space: " << increase_space << endl;
    int i = 0;
    int slot_offset;
    while(i < record_num){
        memcpy(&slot_offset, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i+1), sizeof(int));
        
        if(slot_offset > update_slot_offset){
            //cout << "moved offset : " << slot_offset << endl;
            int new_slot_offset = slot_offset + increase_space;
            //cout << "new offset : " << new_slot_offset << endl;
            int record_length;
            memcpy(&record_length, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i+1) + sizeof(int), sizeof(int));
            //cout << "record length: " << record_length << endl;
            // paste the record to the new space
            memcpy((char *)page + new_slot_offset, (char *)temp_page + slot_offset, record_length );
            // change the slot offset
            memcpy((char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i+1), &new_slot_offset, sizeof(int));

        }
        ++i;
    }
    // paste the update record
    //cout << " update record new offset: " << update_slot_offset << endl;
    if (update_recordSize != 0){
        
        memcpy((char *)page + update_slot_offset, (char *)update_record, update_recordSize);
    
        
    }
    // if update record size == 0, means delete the record, so the slot offset should set to -1
    else{
        int null_offset = -1;
  
        memcpy((char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum), &null_offset, sizeof(int));
    }

        
    // change the length of the new record
    memcpy((char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum)+ sizeof(int), &update_recordSize, sizeof(int));

    //cout << "organized page: " ;
   // printBytes(page, PAGE_SIZE);
    // adjust the free space on the page
    free_space -= increase_space;
    memcpy((char *)page + PAGE_SIZE - sizeof(int), &free_space,sizeof(int));
    //printBytes(page, PAGE_SIZE);
    free(temp_page);

}

bool RecordBasedFileManager::isTomb(FileHandle &fileHandle, RID rid){
    void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    int slot_offset;
    memcpy(&slot_offset, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum), sizeof(int));
    int record_length;
    memcpy(&record_length, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum) + sizeof(int), sizeof(int));
    
    int field_size;
    memcpy( &field_size, (char *)page + slot_offset, sizeof(int));
    free(page);
    return field_size == -1;

}
RID RecordBasedFileManager::getRidFromTombStone(FileHandle &fileHandle, RID rid){
    void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    int slot_offset;
    memcpy(&slot_offset, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum), sizeof(int));
    int record_length;
    memcpy(&record_length, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum) + sizeof(int), sizeof(int));
    

    RID tomb_rid;
    int field_size;
    memcpy( &field_size, (char *)page + slot_offset, sizeof(int));
    int pageNum;
    memcpy( &pageNum, (char *)page + slot_offset + sizeof(int), sizeof(int));
    int slotNum;
    memcpy( &slotNum, (char *)page + slot_offset + sizeof(int)*2, sizeof(int));
    tomb_rid.pageNum = pageNum;
    tomb_rid.slotNum = slotNum;
    free(page);
    return tomb_rid;
}

bool RecordBasedFileManager::isDeleted(FileHandle &fileHandle, RID rid){
     void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    int slot_offset;
    memcpy(&slot_offset, (char *)page + PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(rid.slotNum), sizeof(int));
    free(page);
    return slot_offset == -1;
}



void RecordBasedFileManager::insertByRID(FileHandle &fileHandle, RID insertPlace, int& next_record_pos, int& recordSize, void* record){
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(insertPlace.pageNum, page);

    int free_space;
    memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));

    int record_num;
    memcpy(&record_num, (char *)page + PAGE_SIZE - sizeof(int)*2, sizeof(int));

    // find the offset to insert by traversing all the slot offset to find the maximun value
    int next_slot_pos = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(insertPlace.slotNum+1);

    // paste the record
    memcpy((char *)page + next_record_pos, record, recordSize );

    //paste the offset to slot
    memcpy((char *)page + next_slot_pos, &next_record_pos, sizeof(int));

    // paste the length of record to slot
    memcpy((char *)page + next_slot_pos + sizeof(int), &recordSize, sizeof(int));

    // change the record number and free space, either append the new slot or use unused slot(offset == -1)
    if(insertPlace.slotNum == record_num){
        record_num++;
        free_space -= (sizeof(int)*2 + recordSize);
    }
    else free_space -= recordSize;

    // modified the record num
    memcpy((char *)page + PAGE_SIZE - sizeof(int)*2, &record_num, sizeof(int));

    // modified free space
    memcpy((char *)page + PAGE_SIZE - sizeof(int), &free_space, sizeof(int));

    // write the page to file.
    fileHandle.writePage(insertPlace.pageNum, page);
    free(page);
}


// ================================ RBFM_ScanIterator ================================

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator){
    // consider the case of null field, tombstone, null_data slot
    rbfm_ScanIterator.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return 0;
}

// ================================ RBFM_ScanIterator ================================

void RBFM_ScanIterator::init(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string& conditionAttribute, const CompOp compOp, const void *value, const vector<string>& attributeNames){
    _rbfm = RecordBasedFileManager::instance();
    _fileHandle = fileHandle;
    //_fileHandle.readPage(0, _pgData);
    _recordDescriptor = recordDescriptor;
    _rid.pageNum = 0;
    _rid.slotNum = 1;
    _conditionAttribute = conditionAttribute;
    _compOp = compOp;
    _value = value;
    _attributeNames = attributeNames;
}

bool RBFM_ScanIterator::compByCompOp(int opRes){
    if(opRes == 0 && (_compOp == EQ_OP || _compOp == LE_OP || _compOp == GE_OP)) return true;
    if(opRes == 1 && (_compOp == GT_OP || _compOp == GE_OP || _compOp == NE_OP)) return true;
    if(opRes == -1 && (_compOp == LT_OP || _compOp == LE_OP || _compOp == NE_OP)) return true;
    return false; // if neither the cases above satisfy or compOp is NO_OP, return false
}

// v > vToComp => opRes=1, v = vToComp => opRes=0, v < vToComp => opRes=-1
bool RBFM_ScanIterator::compValueInt(int& v, int& vToComp, AttrType tp){
    int opRes;
    opRes = v > vToComp ? 1 : (v < vToComp ? -1 : 0);
    return compByCompOp(opRes);
}

bool RBFM_ScanIterator::compValueFloat(float& v, float& vToComp, AttrType tp){
    float opRes;
    opRes = v > vToComp ? 1 : (v < vToComp ? -1 : 0);
    return compByCompOp(opRes);
}

bool RBFM_ScanIterator::compValueStr(char* v, char* vToComp, AttrType tp){
    int opRes;
    //opRes = strcmp((const char*)v, (const char*)vToComp);
    opRes = strcmp(v, vToComp);
    return compByCompOp(opRes);
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    int lastPageNum = _fileHandle.getNumberOfPages()-1;
    // search pages by pages
    //cout << _rid.pageNum << " " << _rid.slotNum << " " << lastPageNum << endl;
    while(_rid.pageNum <= lastPageNum && lastPageNum != -1){
        // search slot by slot
        void* page = malloc(PAGE_SIZE);
        //memset(page, 0, PAGE_SIZE);
        _fileHandle.readPage(_rid.pageNum, page);

        int record_num;
        memcpy(&record_num, (char *)page + PAGE_SIZE - sizeof(int)*2, sizeof(int));

        // cout << record_num << " " << _rid.slotNum << endl;
        while( _rid.slotNum <= record_num){
        	//cout << "pageNum is: " << _rid.pageNum << " slotNum is: " <<  _rid.slotNum << " Total slot is: " << record_num << " last page is: " << lastPageNum << endl;
            void* recordAttr = malloc(100);
            RC rc;
            rc = _rbfm->readAttribute(_fileHandle, _recordDescriptor, _rid, _conditionAttribute, recordAttr);
            if (rc == -1){
                // this slot has been deleted
                ++_rid.slotNum;
                free(recordAttr);
                continue;
            }
            // get null indicator. Since there is only 1 attribute, we just need to check 1 byte
            void* nullField = malloc(sizeof(char));
            memcpy(nullField, recordAttr, sizeof(char));
            vector<bool> isNull;
            _rbfm->getNullVector(1, nullField, isNull);
            free(nullField);

            if(isNull[0]){
                free(recordAttr);
                ++ _rid.slotNum;
                continue;
            }

            // retrieve the attribute
            // use compFlag to indicate whether the scanned attribute satisfy filter condition
            bool compFlag = false;
            for(int i=0; i < _recordDescriptor.size(); i++){
                // If not conditionAttribute, skip
                if(_recordDescriptor[i].name != _conditionAttribute) continue;

                if(_recordDescriptor[i].type == TypeInt){
                    int value;
                    memcpy(&value, (char *)recordAttr + sizeof(char), sizeof(int));

                    int vToComp;
                    memcpy(&vToComp, (char *)_value, sizeof(int));
                    // compare value
                    compFlag = compValueInt(value, vToComp, _recordDescriptor[i].type);
                }
                else if (_recordDescriptor[i].type== TypeReal){
                    float value;
                    memcpy(&value, (char *)recordAttr + sizeof(char), sizeof(float));

                    float vToComp;
                    memcpy(&vToComp, (char *)_value, sizeof(float));
                    compFlag = compValueFloat(value, vToComp, _recordDescriptor[i].type);
                }
                else if (_recordDescriptor[i].type== TypeVarChar){
                	// STRANG HERE, why use short is correct?
                    short nameLength;
                    memcpy(&nameLength, (char *)recordAttr + sizeof(char), sizeof(short));
                    //cout << nameLength << endl;
                    int nameLInt = nameLength;
                    char* value = new char[nameLength];
                    memcpy(value, (char *)recordAttr + sizeof(char) + sizeof(int), nameLength);
                    //for(int j = 0; j < nameLength; ++j) cout << value[j];
                    //cout << endl;

                    short vLeng;
                    memcpy(&vLeng, (char *)_value, sizeof(short));
                    //cout << vLeng << endl;
                    int vLInt = vLeng;

                    char* vToComp = new char[vLeng];
                    memcpy(vToComp, (char *)_value+ + sizeof(int), vLeng);
                    //for(int j = 0; j < vLeng; ++j) cout << vToComp[j];
                    //cout << endl;

                    // LOOK OUT
                    //cout << "value length is: " << nameLInt << endl;
                    //cout << "vToComp length is: " << vLInt << endl;
					// nameLength
					int opRes = 0;
					int j = 0, k = 0;
					while(j < nameLength && k < vLeng){
						if(value[j] > vToComp[j]){
							opRes = 1;
							break;
						}
						else if(value[k] < vToComp[k]){
							opRes = -1;
							break;
						}
						++j;
						++k;
					}

					// length are equal or one of string reach end
					if(opRes == 0){
						if(nameLength > vLeng) compFlag = compByCompOp(1);
						else if(nameLength < vLeng) compFlag = compByCompOp(-1);
						else compFlag = compByCompOp(0);
					}
					else compFlag = compByCompOp(opRes);
                    //compFlag = compValueStr(value, vToComp, _recordDescriptor[i].type);

                    delete [] value;
                    delete [] vToComp;
                }
                break;
            }
            free(recordAttr);

            // if not match the condition, try the next slot
            if(!compFlag && _compOp != NO_OP){
                ++ _rid.slotNum;
                continue;
            }

            // the rid where we found the matching slot
            //cout << "Find matching at RID: " << _rid.pageNum << " " << _rid.slotNum << endl;


            // if match the condition, extract the data by _attributeNames

            // get slot position by slot number
            int slot_pos = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*(_rid.slotNum)*2;
            int record_pos;
            memcpy(&record_pos, (char *)page + slot_pos , sizeof(int));
            int recordSize;
            memcpy(&recordSize, (char *)page + slot_pos + sizeof(int) , sizeof(int));
            //cout << "slot position: " << slot_pos << " Record Size is: " << recordSize << endl;
            // get record
            void* record = malloc(recordSize);
            memset(record, 0, recordSize);
            memcpy((char *)record, (char *)page + record_pos, recordSize);
            

            // get the field size
            int field_size;
            memcpy( &field_size, (char *)record, sizeof(int));
            //cout << "field size: " << field_size << endl;

            // store the offset and length of the field in the array
            short* offset_arr = new short[field_size];
            short* length_arr = new short[field_size];
            for (int i =0; i < field_size; i++){
                memcpy(&offset_arr[i], (char *)page+ record_pos + 4 + i*sizeof(short)*2 ,sizeof(short));
            }
            for (int i =0; i < field_size; i++){
                memcpy(&length_arr[i], (char *)page+record_pos + 4 + 2 + i*sizeof(short)*2 ,sizeof(short));
            }

            // record the index of _attributeNames in _recordDescriptor;
            vector<int> attributeIdx;
            for(int i = 0, ai = 0; (i < _recordDescriptor.size() && ai < _attributeNames.size()); i++){
                if(_recordDescriptor[i].name == _attributeNames[ai]){
                	//cout << i << " " << _recordDescriptor[i].name << endl;
                    ++ai;
                    attributeIdx.push_back(i);
                }
            }
            int aN = attributeIdx.size();

            // construct null field s
            int nullIndicatorLength = ceil(aN/8.0);
            char *null_Field = new char[nullIndicatorLength]();

            for (int i =0; i < nullIndicatorLength; i++){
                for(int j=0; j < 8; j++){
                    // if offset arr is 0, this field is null
                    if(i*8+j < aN && offset_arr[attributeIdx[i*8+j]] == 0){
                        null_Field[i] = null_Field[i] | (1 << (8-j-1));
                    }
                }
            }

            int buffer_offset = 0;

            // paste the null field to data
            for (int i =0 ; i < nullIndicatorLength; i++){
                memcpy((char*)data + buffer_offset, null_Field+i, sizeof(char));
                buffer_offset += 1;
            }
            for(int ai = 0; ai < aN; ++ai){
                // access the index of wanted descriptor
                int i = attributeIdx[ai];
                // access the data position by offset array
                char* paste_pos = (char *)record + offset_arr[i];
                //cout << "offset of i is: " << i <<  ' ' << offset_arr[i] << endl;

                if(offset_arr[i] == 0) continue;
                if(_recordDescriptor[i].type == 0){
                    memcpy((char *)data + buffer_offset, paste_pos, sizeof(int));
                    buffer_offset += sizeof(int);
                }
                else if (_recordDescriptor[i].type == 1){
                    memcpy((char *)data+ buffer_offset, paste_pos, sizeof(float));
                    buffer_offset += sizeof(float);

                }
                else if (_recordDescriptor[i].type == 2){
                    // for varchar we need to copy name length
                    int nameLength = 0;
                    nameLength = int(length_arr[i]);
                    //cout << nameLength << endl;
                    memcpy((char *)data + buffer_offset, &nameLength, sizeof(int));
                    buffer_offset += sizeof(int);

                    // copy data
                    memcpy((char *)data + buffer_offset, paste_pos, nameLength);
                    buffer_offset += nameLength;
                }
            }

            delete [] offset_arr;
            delete [] length_arr;

            // set the input
            rid.pageNum = _rid.pageNum;
            rid.slotNum =  _rid.slotNum;

            // update the class attribute for the next record searching
            ++_rid.slotNum;


            free(null_Field);
            free(record);
            free(page);
            return 1;
        }
        free(page);
        ++_rid.pageNum;
        _rid.slotNum = 1;
    }
    return RBFM_EOF;
}
RC RBFM_ScanIterator::close() {
	_rbfm->closeFile(_fileHandle);
	return 0;
}


