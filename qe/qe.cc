
#include "qe.h"
#include <math.h>

#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <functional>
#include <map>
#include <typeinfo>
#include <limits>
using namespace std;


/*
This code reference from 
https://stackoverflow.com/questions/6684573/floating-point-keys-in-stdmap

usage:
map<double,double,own_double_less> mymap;
*/





class BlockIO
{
    public:
        FILE *pfile;
        string fileName;
        int out_offset;
        int read_pages;
        int total_pages;
        void* cachePage;
        int read_offset;
        int readPageSize;

        BlockIO();
        ~BlockIO();

        void createFile(string fn);
        void openFile(string fn);
        void deleteFile(string fn);
        void closeFile(string fn);
        void writeData(void* data, int dataSize, bool write2page);
        RC readData(void* data, int& dataSize);
};
// void BlockIO::createFile(string fn){
//     pfile = fopen(fn.c_str(),"r+");
// }

BlockIO::BlockIO(){
    out_offset = sizeof(int);
    read_pages = 0;
    total_pages = 0;
    cachePage = malloc(PAGE_SIZE);
    read_offset = 0;
}

BlockIO::~BlockIO(){
    free(cachePage);
}





void BlockIO::openFile(string fn){
    pfile = fopen(fn.c_str(), "r+");
}
void BlockIO::closeFile(string fn){
    fclose(pfile);
}
void BlockIO::deleteFile(string fn){
    remove(fn.c_str());
}
void BlockIO::writeData(void* data, int dataSize, bool write2page){
    if(write2page && out_offset > sizeof(int)){
        // the is called when writing last record
        memcpy((char *)cachePage, &out_offset, sizeof(int));
        fseek(pfile, 0, SEEK_END);
        fwrite(cachePage, sizeof(char), PAGE_SIZE, pfile);
        total_pages += 1;
        memset(cachePage, 0, PAGE_SIZE);

        return;
    }
    if (out_offset + dataSize > PAGE_SIZE - sizeof(int)){
        memcpy((char *)cachePage, &out_offset, sizeof(int));
        fseek(pfile, 0, SEEK_END);
        fwrite(cachePage, sizeof(char), PAGE_SIZE, pfile);
        out_offset = sizeof(int);
        memset(cachePage, 0, PAGE_SIZE);
        total_pages += 1;
    }
    memcpy((char *)cachePage+out_offset, (char *)data, dataSize);
    out_offset += dataSize;
    
}
RC BlockIO::readData(void* data, int& dataSize){
    if (read_pages >= total_pages && read_offset >= readPageSize){
        // cout << "buffer offset: " << read_offset << "buffer size: " << readPageSize << endl;
        return QE_EOF;
    }
    if(read_pages == 0 || read_offset >= readPageSize){
        pfile = fopen(fileName.c_str(), "r");
        fseek(pfile, read_pages*PAGE_SIZE, SEEK_SET);
        fread(cachePage, sizeof(char), PAGE_SIZE, pfile);
        fclose(pfile);
        read_pages += 1;
        memcpy(&readPageSize, (char *)cachePage, sizeof(int));
        read_offset = sizeof(int);
    }

    int tupleLength;
    memcpy(&tupleLength, (char *)cachePage + read_offset, sizeof(int));
    memcpy((char *)data, (char *)cachePage + read_offset + sizeof(int), tupleLength - sizeof(int));
    read_offset += tupleLength;

    return 0;

}


//  


void getNullVector(int nullIndicatorLength, void* nullField, vector<bool> &isNull){
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


/*

Input: TA's format

The format of the output:
|length of record| offset 1| offset 2| ..| .. | data1 | data2 | ... 

*/
void interpretTuple(const vector<Attribute> &tupleDescriptor, const void* data, void* tuple, int &tupleSize, int &dataSize){
    /*
    This function interpret TA's record format into the format that we should store in pages.
    TA foramt:  [null field ] .. [data field], [data field]...
    */
    // get the data size by recordDescriptor first
    int field_size = tupleDescriptor.size();


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
    for(int i = 0; i < tupleDescriptor.size(); i++){
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
    for(i = 0, it=tupleDescriptor.begin(); it!=tupleDescriptor.end(); it++, i++){
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
    memcpy((char *)tuple, &field_size, sizeof(int));
    tmp_offset += sizeof(int);

    // paste the offset directory to record
    for(int i = 0; i < offset_array.size(); i++){
        memcpy((char *)tuple + tmp_offset, &offset_array[i], sizeof(short));
        tmp_offset += sizeof(short);
        memcpy((char *)tuple + tmp_offset, &length_array[i], sizeof(short));
        tmp_offset += sizeof(short);
    }
    // paste on the buffer
    memcpy((char* )tuple + tmp_offset, (char *)buffer, data_size);

    tupleSize = offset;
    dataSize = data_offset;
    free(buffer);
    free(nullField);
}

void recordToTA(const vector<Attribute> &tupleDescriptor, void* tuple, void* data, int &dataSize){
    // get the field size
    int field_size;
    memcpy( &field_size, (char *)tuple, sizeof(int));
    // cout << 

    //cout << "Reading Record, page num: " << pageNum << " slotNum: " << slotNum << " get page count: " << fileHandle.getNumberOfPages() << " slot pos: " << slot_pos << " record pos: " << record_pos << endl;
    //cout << "free space: " << free_space << " "  << "record offset: " << record_pos <<  " record size: " << recordSize << endl;

    
    // store the offset of the field in the array
    short* offset_arr = new short[field_size];
    short* length_arr = new short[field_size];

    for (int i =0; i < field_size; i++){
        memcpy(&offset_arr[i], (char *)tuple + 4 + i*sizeof(short)*2 ,sizeof(short));
    }
    
    for (int i =0; i < field_size; i++){
        memcpy(&length_arr[i], (char *)tuple + 4 + 2 + i*sizeof(short)*2 ,sizeof(short));
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
        memcpy((char*)data + buffer_offset, (char *)nullField+i, sizeof(char));
        buffer_offset += 1;
    }
    
    // paste the record to data;
    int i;
    for(i=0, it=tupleDescriptor.begin(); it!=tupleDescriptor.end(); it++, i++){

        char* paste_pos = (char *)tuple + offset_arr[i];
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
    dataSize = buffer_offset;
    delete [] nullField;
    delete [] offset_arr;
    delete [] length_arr;

}


void readAttribute(vector<Attribute> &attrs, string attributeName, const void* data,void* attrValue, int& attrLength){
    void* tuple = malloc(PAGE_SIZE);
    int tupleSize, dataSize;

    interpretTuple(attrs, data,tuple,tupleSize, dataSize);

    int target_field = -1;
    AttrType type;
    // scan the attrs to get the index of target attribute index
    for(int i =0; i < attrs.size();i++){
        if(attrs[i].name == attributeName){
            target_field = i;
            type = attrs[i].type;
            break;
        }
    }
    short offset;
    short length;
    memcpy(&offset, (char *)tuple + sizeof(int) + sizeof(short)*2*(target_field), sizeof(short));
    memcpy(&length, (char *)tuple + sizeof(int) + sizeof(short)*2*(target_field) + sizeof(short), sizeof(short));
    int l = (int)length;

    // construct null field
    char nullField = ((offset == 0) << 7);
    int data_offset = 0;
    memcpy((char *)attrValue, &nullField, sizeof(char));
    data_offset += sizeof(char);

    if(type == TypeVarChar){
        memcpy((char *)attrValue + data_offset, &l, sizeof(int));
        data_offset += sizeof(int);
    }
    memcpy((char *)attrValue+data_offset, (char *)tuple+offset, l);
    data_offset += l;
    attrLength = data_offset;
    free(tuple);

}


void combineTuple(void* lhsTuple, vector<Attribute> leftAttrs, int lhsSize, void* rhsTuple, vector<Attribute> rightAttrs, int rhsSize, void* result, int &resultSize){
    int lhsNullLength = ceil((double)leftAttrs.size()/8.0);
    int rhsNullLength = ceil((double)rightAttrs.size()/8.0);

    vector<bool> lhsNullVector;
    vector<bool> rhsNullVector;
    // cout << rhsNullLength << endl;
    void* lhsNullField = malloc(PAGE_SIZE);
    void* rhsNullField = malloc(PAGE_SIZE);
    memcpy((char *)lhsNullField, (char *)lhsTuple, lhsNullLength);
    memcpy((char *)rhsNullField, (char *)rhsTuple, rhsNullLength);

    getNullVector(lhsNullLength, lhsNullField, lhsNullVector );
    getNullVector(rhsNullLength, rhsNullField, rhsNullVector );

    // extend lhs with rhs
    lhsNullVector.insert(lhsNullVector.end(), rhsNullVector.begin(), rhsNullVector.end());
    vector<bool> combineNullVector;
    for(int i = 0; i < leftAttrs.size(); i++) combineNullVector.push_back(lhsNullVector[i]);
    for(int i = 0; i < rightAttrs.size(); i++) combineNullVector.push_back(rhsNullVector[i]);

    // construct new null indicator
    int combineNullLength = ceil((double)combineNullVector.size()/8.0);
    char* combineNullIndi = new char[combineNullLength]();
    for(int i = 0; i < combineNullLength; i++){
        for(int j = 0; j < 8; j++){
            if(i*8+j < combineNullVector.size() && combineNullVector[i*8+j] == 1){
                combineNullIndi[i] = combineNullIndi[i] | (1 << 8-j-1);
            }
        }
    }
    // cout << "combine null indicator: ";
    // for(auto nu: lhsNullVector){
    //     cout << nu;
    // }
    // cout << endl;
    int result_offset = 4;
    memcpy((char *)result+result_offset, (char *)combineNullIndi, combineNullLength);
    result_offset += combineNullLength;
    memcpy((char *)result+result_offset, (char *)lhsTuple+lhsNullLength, lhsSize - lhsNullLength);
    result_offset += lhsSize - lhsNullLength;
    memcpy((char *)result+result_offset, (char *)rhsTuple+rhsNullLength, rhsSize - rhsNullLength);
    result_offset += rhsSize - rhsNullLength;
    memcpy((char *)result, &result_offset, sizeof(int));
    resultSize = result_offset;
    free(lhsNullField);
    free(rhsNullField);

}

Filter::Filter(Iterator* input, const Condition &condition) {
    iter = input;
    iter->getAttributes(outAttrs);
    _condition = condition;
    _compOp = condition.op;
    // rbfm = RecordBasedFileManager::instance();
    //rm = input->rm;
    
    // if the input iterator is index scan, we should set the boundary
    // if(typeid(*iter) == typeid(IndexScan)){
    //     IndexScan *it = iter;
    //     // iter = (IndexScan) iter;
    //     // if(_compOp == EQ_OP){
    //     //     iter->setIterator(_condition.rhsValue.data, _condition.rhsValue.data, true, true);
    //     // }
    //     // else if(_compOp == LT_OP){
    //     //     iter->setIterator(NULL, _condition.rhsValue.data, false, false);

    //     // }
    //     // else if(_compOp == LE_OP){
    //     //     iter->setIterator(NULL, _condition.rhsValue.data, false, true);
    //     // }
    //     // else if(_compOp == GT_OP){
    //     //     iter->setIterator(_condition.rhsValue.data, NULL, false, false);
    //     // }
    //     // else if(_compOp == GE_OP){
    //     //     iter->setIterator(_condition.rhsValue.data, NULL, true, false);
    //     // }
    //     // else{
    //     //     iter->setIterator(NULL, NULL, false, false);
    //     // }


    // }

}
Filter::~Filter(){
    
}

RC Filter::getNextTuple(void* data){
    // void* buffer = malloc(PAGE_SIZE);
    void* attrValue = malloc(PAGE_SIZE);
    while (iter->getNextTuple(data) != IX_EOF){
        //rid = iter->rid;
        // void* attrValue = malloc(PAGE_SIZE);
        int l;
        
        readAttribute(outAttrs, _condition.lhsAttr, data, attrValue, l);
        
        bool compFlag = false;
        if(_condition.rhsValue.type == TypeInt){
            int value;
            memcpy(&value, (char *)attrValue+sizeof(char), sizeof(int));
            compFlag = compValueInt(value, *(int *)_condition.rhsValue.data, TypeInt);
        }
        else if(_condition.rhsValue.type == TypeReal){
            float value;
           
            // cout << "rhs value: " << *(float *)_condition.rhsValue.data << endl;
            memcpy(&value, (char *)attrValue+sizeof(char), sizeof(int));
            //  cout << "lhs value: " << value << endl;
            compFlag = compValueFloat(value, *(float *)_condition.rhsValue.data, TypeReal);
        }
        else if(_condition.rhsValue.type == TypeVarChar){
            compFlag = compValueStr(attrValue, _condition.rhsValue.data, TypeVarChar);

        }
        
        if(compFlag || _compOp == NO_OP){
            free(attrValue);
            return 0;
        }

    }
    free(attrValue);
    return QE_EOF;
   
    
}
void Filter::getAttributes(vector<Attribute> &attrs) const{
    // iter->getAttributes(attrs);
    attrs = this->outAttrs;
}

bool Filter::compByCompOp(int opRes){
    if(opRes == 0 && (_compOp == EQ_OP || _compOp == LE_OP || _compOp == GE_OP)) return true;
    if(opRes == 1 && (_compOp == GT_OP || _compOp == GE_OP || _compOp == NE_OP)) return true;
    if(opRes == -1 && (_compOp == LT_OP || _compOp == LE_OP || _compOp == NE_OP)) return true;
    return false; // if neither the cases above satisfy or compOp is NO_OP, return false
}
// overload
bool Filter::compValueInt(int& v, int& vToComp, AttrType tp){
    int opRes;
    opRes = v > vToComp ? 1 : (v < vToComp ? -1 : 0);
    return compByCompOp(opRes);
}
// overload
bool Filter::compValueFloat(float& v, float& vToComp, AttrType tp){
    float opRes;
    opRes = v > vToComp ? 1 : (v < vToComp ? -1 : 0);
    return compByCompOp(opRes);
}

bool Filter::compValueStr(void* v, void* vToComp, AttrType tp){
    // int opRes;
    //opRes = strcmp((const char*)v, (const char*)vToComp);
    // opRes = strcmp(v, vToComp);
    int nameLength;
    memcpy(&nameLength, (char *)v + sizeof(char), sizeof(int));
    
    char* strValue = new char[nameLength];
    memcpy((char *)strValue, (char *)v + sizeof(char) + sizeof(int), nameLength);
    
    int vLeng;
    memcpy(&vLeng, (char *)_condition.rhsValue.data, sizeof(int) );

    char* strToComp = new char[vLeng];
    memcpy(strToComp, (char *)vToComp+ sizeof(int), vLeng);
    int opRes = 0;
    int j = 0, k = 0;
    while(j < nameLength && k < vLeng){
        if(strValue[j] > strToComp[j]){
            opRes = 1;
            break;
        }
        else if(strValue[k] < strToComp[k]){
            opRes = -1;
            break;
        }
        ++j;
        ++k;
    }
    delete [] strValue;
    delete [] strToComp;

    if(opRes == 0){
        if(nameLength > vLeng) return compByCompOp(1);
        else if(nameLength < vLeng) return compByCompOp(-1);
        else return compByCompOp(0);
    }
    else return compByCompOp(opRes);
    
    return compByCompOp(opRes);
}


Project::Project(Iterator *input,const vector<string> &attrNames) {
    iter = input;
    iter->getAttributes(attrs);
    map<string, int> pool;
    for(auto attr: attrNames){
        pool[attr] = 1;
    }
    for(auto attr: attrs){
        if(pool.find(attr.name) != pool.end()){
            projectAttrs.push_back(attr);
            // cout << "projectAttrs: " << attr.name << endl;
        }
    }
    targetAttrNames = attrNames;

}
Project::~Project(){

}

RC Project::getNextTuple(void* data){
    void* buffer = malloc(PAGE_SIZE);
    RC rc;
    rc = iter->getNextTuple(buffer);

    if(rc == QE_EOF){
        free(buffer);
        return QE_EOF;
    }

    // void* tuple = malloc(PAGE_SIZE);
    // int tupleSize, dataSize;
    // interpretTuple(attrs, buffer, tuple, tupleSize, dataSize);

    // 
    // unordered_map<string, int> map;
    // for(int i = 0; i < targetAttrNames.size();i++){
    //     map.insert({targetAttrNames[i],i});
    // }

    // get the value of project attributes
    vector<pair<void*, int> > values;
    vector<bool> isNull;
    for( int i =0; i < projectAttrs.size(); i ++){
        // if(map.find(projectAttrs[i].name) != map.end()){
        void* temp = malloc(PAGE_SIZE);
        // read the attribute into temp
        // temp format is the same of TA's
        // |null indicator| data1 | data2 | ...
        int l;
        readAttribute(attrs, projectAttrs[i].name, buffer, temp, l);
        bool isN = false;
        if(*(unsigned char *)temp != 0){
            isN = true;
        }
        isNull.push_back(isN);
        void* attrValue = malloc(l-sizeof(char));
        memcpy((char *)attrValue, (char *)temp + sizeof(char), l-sizeof(char));
        values.push_back({attrValue, l-sizeof(char)});// delete the null indicator length
        
        free(temp);
        //}
    }

    // construct null indicator
    // Null field length
    int nullIndicatorLength = ceil((double)values.size()/8.0);
    // get null indicator
    char* nullField = new char[nullIndicatorLength]();

    for(int i=0; i < nullIndicatorLength; i++){
        for(int j=0; j < 8; j++){
            int idx = i*8+j;
            if(idx < isNull.size()){
                if(isNull[idx]){
                    nullField[i] = nullField[i] || (1 << (8-j-1));
                }
            }
        }
    }

    // construct data
    int data_offset = 0;
    // paste the null field to data
    for(int i=0; i < nullIndicatorLength; i++){
        memcpy((char *)data+data_offset, nullField+i, sizeof(char));
        data_offset += 1;
    }
    // paste the tuple to data

    for(int i=0; i < values.size(); i++){
        memcpy((char *)data+data_offset, (char *)values[i].first, values[i].second);
        data_offset += values[i].second;
    }
    delete [] nullField;
    free(buffer);

    return 0;

}
void Project::getAttributes(vector<Attribute> &attrs) const{
    attrs = this->projectAttrs;
}



BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        )
{
    // create 
    // RelationManager *rm = RelationManager::instance();
    
    // initialize parameters
    _condition = condition;
    this->numPages = numPages;

    // get left attrNames and right attrNames
    leftIn->getAttributes(leftAttrs);// in leftAttrs, will be left.B
    rightIn->getAttributes(rightAttrs); // in rightAttr, will be right.B
    
    // get type of left Attr by lhsAttr
    Attribute lAttr, rAttr; // the attribute of right and left 
    
    for(auto attr: leftAttrs){
        if(attr.name == _condition.lhsAttr){
            lAttr = attr;
        }
        joinAttrs.push_back(attr);
    }
    for(auto attr: rightAttrs){
        if(attr.name == _condition.rhsAttr){
            rAttr = attr;
        }
        joinAttrs.push_back(attr);
    }
    compType = lAttr.type;
    if(compType != rAttr.type){
        // if rhs type is not the same with lhs type
        return;
    }

    // define output table name and create table
    outTableName = _condition.lhsAttr + "_join_" + _condition.rhsAttr;
    // rm->createTable(outTableName, joinAttrs);

    // The table is a page list
    // every page with record format
    // | record1 | record2 | record3 | record4 | ....
    // get the length of record with the first 4 byte in every record
    // the format is same as TA's format
    
    pFile = fopen(outTableName.c_str(), "r");
    if (pFile != NULL){ //exists
        fclose(pFile);
        return;
    }
    
    pFile = fopen(outTableName.c_str(), "wb");

    totalPage = 0;
    
    // type see whether string can work
    unordered_map<char*, vector<void*> > mymap;





    // first load the pages until memory full
    int K_capacity = (this->numPages - 2)*PAGE_SIZE;

    void* outPage = malloc(PAGE_SIZE);
    int out_offset = sizeof(int);
   
    RC leftRc = 0, rightRc = 0;
    int block_Read = 0;
    while (leftRc != QE_EOF){
        int read = 0;
        while (read < K_capacity){
            void *data = malloc(PAGE_SIZE);
            leftRc = leftIn->getNextTuple(data);
            if(leftRc == QE_EOF){
                free(data);
                break;
            }


            void *tuple = malloc(PAGE_SIZE);
           
            int tupleSize, dataSize;
            interpretTuple(leftAttrs, data, tuple, tupleSize, dataSize);
            
            void* attrValue = malloc(PAGE_SIZE);
            int attrLen;
            readAttribute(leftAttrs, _condition.lhsAttr, data, attrValue, attrLen);
            // change attrValue to string
            char* keyChar = new char[attrLen];
            memcpy(&keyChar, (char *)attrValue, attrLen);
            
            // store value to map
            // mymap[leftKey].push_back(tuple);
            mymap[keyChar].push_back(tuple);
            // mymap[leftKey].push_back(data);

            read += tupleSize;
            // free(tuple);
            free(data);
            free(attrValue);


            
        }
        block_Read++;
        // get one right tuple and compare
        // if key in map, write it to output
        // if key not in map, skip it
        void* data = malloc(PAGE_SIZE), *rightTuple = malloc(PAGE_SIZE);
        
        while(rightIn->getNextTuple(data) != QE_EOF){
            // rightIn->getNextTuple(data);
            int tupleSize, dataSize;
            interpretTuple(rightAttrs, data, rightTuple, tupleSize, dataSize);
            void* attrValue = malloc(PAGE_SIZE);
            int attrLen;
            readAttribute(rightAttrs, _condition.rhsAttr, data, attrValue, attrLen);
            // change attrValue to string
            char* keyChar = new char[attrLen];
            memcpy(&keyChar, (char *)attrValue, attrLen);
            //string rightKey(keyChar);
            //delete [] keyChar;

            free(attrValue);

            if (mymap.find(keyChar) != mymap.end()){
                
                
                for(auto matched_value: mymap[keyChar]){
                    // matched value is a void* of tuple
                    // combine left and right tuple
                    void* combTuple = malloc(PAGE_SIZE);
                    void* lhsData = malloc(PAGE_SIZE);
                    void* rhsData = malloc(PAGE_SIZE);
                    int lhsSize=0, rhsSize=0;
                    int combineSize;
                    recordToTA(leftAttrs, matched_value, lhsData, lhsSize);
                    recordToTA(rightAttrs, rightTuple, rhsData, rhsSize);

                    combineTuple(lhsData, leftAttrs, lhsSize, rhsData, rightAttrs,rhsSize, combTuple, combineSize );
                    
                    // write to buffer page

                    if (out_offset + combineSize > PAGE_SIZE - sizeof(int)){
                    // write the buffer page back to disk, set out_offset  to 0
                        memcpy((char *)outPage, &out_offset, sizeof(int));
                        fseek(pFile, 0, SEEK_END);
                        fwrite(outPage, sizeof(char), PAGE_SIZE, pFile);
                        out_offset = sizeof(int);
                        memset(outPage, 0, PAGE_SIZE);
                        totalPage += 1;
                        
                    }

                    memcpy((char *)outPage+out_offset, (char *)combTuple, combineSize);
                    out_offset += combineSize;
                    free(lhsData);
                    free(rhsData);
                    free(combTuple);
                }

            }
           
        }     
        // 1. Set rightIn interator
        rightIn->setIterator();
        // 2. free the unorder_map;
        for(auto vec: mymap){
            for(auto item: vec.second){// vec.second is the value
                free(item);
            }
            // free(vec.first);
        }
        mymap.clear();
        free(data);
        free(rightTuple);

    }
    if(out_offset > sizeof(int)){
        memcpy((char *)outPage, &out_offset, sizeof(int));
        fseek(pFile, 0, SEEK_END);
        fwrite(outPage, sizeof(char), PAGE_SIZE, pFile);
        totalPage += 1;
    }
    // cout << totalPage << endl;
    // cout << "block read: " << block_Read << endl;
    free(outPage);

    fclose(pFile);
    pFile = NULL;
    pfilePage = 0;
    outBuffer = NULL;
    




}
BNLJoin::~BNLJoin(){
    remove(outTableName.c_str());
    if(outBuffer != NULL) free(outBuffer);
    

}
RC BNLJoin::getNextTuple(void* data){
    /*
    This is a work around.
    Maybe the better method is 
    */

    if (pfilePage >= totalPage && bufferOffset >= bufferSize){
        // cout << "buffer offset: " << bufferOffset << "buffer size: " << bufferSize << endl;
        return QE_EOF;
    }

    if(outBuffer == NULL || bufferOffset >= bufferSize){
        if (outBuffer == NULL) outBuffer = malloc(PAGE_SIZE);
        pFile = fopen(outTableName.c_str(), "r");
        fseek(pFile, pfilePage*PAGE_SIZE, SEEK_SET);
        fread(outBuffer, sizeof(char), PAGE_SIZE, pFile);
        fclose(pFile);

        pfilePage += 1;
        memcpy(&bufferSize, (char *)outBuffer, sizeof(int));
        bufferOffset = sizeof(int);

    }

    int tupleLength;
    memcpy(&tupleLength, (char *)outBuffer + bufferOffset, sizeof(int));
    memcpy((char *)data, (char *)outBuffer + bufferOffset + sizeof(int), tupleLength - sizeof(int));
    bufferOffset += tupleLength;

    return 0;

    
    
    
    // fseek(pFile, pfile_page*PAGE_SIZE, SEEK_SET);
    // if (outBuffer == NULL) outBuffer = malloc(PAGE_SIZE);
    // fread(outBuffer, sizeof(char), PAGE_SIZE, pFile);
    // pfile_page += 1;

    // // get the end point of the page
    // int total_length;
    // memcpy(&total_length, (char *)outBuffer, sizeof(int));
    // int read = sizeof(int);
    // while(read < total_length){
    //     int tupleLength;
    //     memcpy((char *)data, (char *)outBuffer + read, tupleLength);
    //     return 0;
    // }



    // //}

    // //fseek(pFile, pfile_offset, SEEK_SET);
    // // int tupleLength;
    // // fread(&tupleLength, sizeof(int), 1, pFile);
    // // fseek(pFile, pfile_offset+1, SEEK_SET);
    // // fread(data, tupleLength, 1, pFile);
    // // pfile_offset += tupleLength;
    // return QE_EOF;

}
void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
    attrs = this->joinAttrs;

}



INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        )
{
     _condition = condition;
    this->leftIn = leftIn;
    this->rightIn = rightIn;

    // get left attrNames and right attrNames
    leftIn->getAttributes(leftAttrs);// in leftAttrs, will be left.B
    rightIn->getAttributes(rightAttrs); // in rightAttr, will be right.B
    
    // get type of left Attr by lhsAttr
    Attribute lAttr, rAttr; // the attribute of right and left 
    
    for(auto attr: leftAttrs){
        if(attr.name == _condition.lhsAttr){
            lAttr = attr;
        }
        joinAttrs.push_back(attr);
    }
    for(auto attr: rightAttrs){
        if(attr.name == _condition.rhsAttr){
            rAttr = attr;
        }
        joinAttrs.push_back(attr);
    }
    compType = lAttr.type;
    if(compType != rAttr.type){
        // if rhs type is not the same with lhs type
        return;
    }
    leftRc = 0;
    rightInEnd = true;
    cacheLhsData = malloc(PAGE_SIZE);
    cacheLhsTuple = malloc(PAGE_SIZE);
    cacheLhsAttrValue = malloc(PAGE_SIZE);
}

INLJoin::~INLJoin(){
    free(cacheLhsData);
    free(cacheLhsTuple);
    free(cacheLhsAttrValue);

}
RC INLJoin::getNextTuple(void* data){
    void *rhsData = malloc(PAGE_SIZE);
    void* rhsTuple = malloc(PAGE_SIZE);
    int rhsDataSize, rhsTupleSize;
    void* rhsAttrValue = malloc(PAGE_SIZE);
    int rhsAttrLength;



    while(leftRc != QE_EOF){ // if leftIn is not at end
        if(rightInEnd){ // if rightIn is EOF, we need to get next left item
            leftRc = leftIn->getNextTuple(cacheLhsData);
            if(leftRc == QE_EOF) {
                free(rhsData);
                free(rhsTuple);
                free(rhsAttrValue);
                return QE_EOF;
            }
            interpretTuple(leftAttrs, cacheLhsData, cacheLhsTuple, cacheLhsTupleSize, cacheLhsDataSize);
            readAttribute(leftAttrs, _condition.lhsAttr, cacheLhsData, cacheLhsAttrValue, cacheLhsAttrLength);
            int nullIndiLen = ceil(leftAttrs.size()/8.0);
            void* key = malloc(cacheLhsAttrLength-nullIndiLen); // ignore the first null indicator field
            memcpy((char *)key, (char *)cacheLhsAttrValue + nullIndiLen, cacheLhsAttrLength-nullIndiLen);
            // set rightIn iterator condition
            rightIn->setIterator(key, key, true, true);
            
        }

        while(rightIn->getNextTuple(rhsData) != QE_EOF){
            rightInEnd = false;
            interpretTuple(rightAttrs, rhsData, rhsTuple, rhsTupleSize, rhsDataSize);
            readAttribute(rightAttrs, _condition.rhsAttr, rhsData, rhsAttrValue, rhsAttrLength);

            if(cacheLhsAttrLength != rhsAttrLength) continue;
            
            int combSize;
            void* combData = malloc(PAGE_SIZE);
            combineTuple(cacheLhsData, leftAttrs, cacheLhsDataSize, rhsData, rightAttrs, rhsDataSize, combData, combSize);
            // ignore the first length field;
            memcpy((char *)data, (char *)combData + sizeof(int), combSize - sizeof(int));
            free(rhsData);
            free(rhsTuple);
            free(rhsAttrValue);
            free(combData);
            return 0;
            // }
            // else continue;
        }
        rightInEnd = true;

    }
    free(rhsData);
    free(rhsTuple);
    free(rhsAttrValue);

    return QE_EOF;



}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
    attrs = this->joinAttrs;

}


Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
){
    this->aggAttr = aggAttr;
    this->_op = op;
    this->getAttributes(outAttrs);
    vector<Attribute> allAttrs;
    input->getAttributes(allAttrs);


    Attribute attr;
    attr.type = TypeReal;
    attr.length = sizeof(float);
    string name = "";
    if(_op == MIN) name += "MIN(";
    else if(_op == MAX) name += "MAX(";
    else if(_op == COUNT) name += "COUNT(";
    else if(_op == SUM) name += "SUM(";
    else if(_op == AVG) name += "AVG(";

    name += aggAttr.name + ")";
    attr.name = name;
    outAttrs.push_back(attr);
    // collect all the attr
    count = 0;
    if(_op == MIN){
        resultValue = numeric_limits<float>::max();
    }
    else if(_op == MAX){
        resultValue = numeric_limits<float>::min();
    }
    else {
        resultValue = 0.0;
    }
    
    void* data = malloc(PAGE_SIZE), *tuple = malloc(PAGE_SIZE);
    int dataSize, tupleSize;

    void* attrValue = malloc(PAGE_SIZE);
    int attrLength;

    while(input->getNextTuple(data) != QE_EOF){
        readAttribute(allAttrs, aggAttr.name, data, attrValue, attrLength);
        float value;
        count++;
        if(aggAttr.type == TypeInt){
            int blob;
            memcpy(&blob, (char *)attrValue + sizeof(char), sizeof(int));
            value = (float)blob;
        }
        else if(aggAttr.type == TypeReal){
            memcpy(&value, (char *)attrValue + sizeof(char), sizeof(float));
        }
        if(_op == MIN){
            resultValue = min(resultValue, value);
        }
        else if(_op == MAX){
            resultValue = max(resultValue, value);
           
        }
        else if(_op == COUNT){
            resultValue = count;
        }
        else if (_op == SUM){
            resultValue += value;
        }
        else if(_op == AVG){
            resultValue += value;
        }

    }
    if(_op == AVG){
        resultValue /= count;
    }
    void* out = malloc(PAGE_SIZE);
    char nullField = 0;
    nullField = ((count == 0) ? 1 : 0) << 7;
    memcpy((char *)out, &nullField, sizeof(char));
    memcpy((char *)out+sizeof(char), &resultValue, sizeof(float));
    output.push_back({sizeof(int) + sizeof(char),out});
    outIt = output.begin();

    free(data);
    free(tuple);
    free(attrValue);
    
}

Aggregate::Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
)
{
    this->aggAttr = aggAttr;
    this->_op = op;
    this->getAttributes(outAttrs);
    this->groupAttr = groupAttr;
    vector<Attribute> allAttrs;
    input->getAttributes(allAttrs);


    Attribute attr;
    attr.type = TypeReal;
    attr.length = sizeof(float);
    string name = "";
    if(_op == MIN) name += "MIN(";
    else if(_op == MAX) name += "MAX(";
    else if(_op == COUNT) name += "COUNT(";
    else if(_op == SUM) name += "SUM(";
    else if(_op == AVG) name += "AVG(";

    name += aggAttr.name + ")";
    attr.name = name;
    
    
    outAttrs.push_back(groupAttr); // the group attr go first, then the aggregate attr
    outAttrs.push_back(attr);

    
    void* data = malloc(PAGE_SIZE);
    void* tuple = malloc(PAGE_SIZE);
    void* attrValue = malloc(PAGE_SIZE);
    int attrLength;
    int dataSize, tupleSize;

    // collect all the tuple and put into hash
    while(input->getNextTuple(data) != QE_EOF){
       
        
        interpretTuple(allAttrs, data, tuple, tupleSize, dataSize);
        readAttribute(allAttrs, groupAttr.name, data, attrValue, attrLength);

        // change attrValue to string
        char* keyChar = new char[attrLength];
        memcpy(&keyChar, (char *)attrValue, attrLength);
        
        // is string constructor copy the data?
        void* indata = malloc(PAGE_SIZE);
        memcpy((char *)indata, (char *)data, dataSize);
        
        // store value to map
        groups[keyChar].push_back(indata);
        
    }
    
    // count the aggregate value

    
    for(auto group: groups){
        count = 0;

        if(_op == MIN){
            resultValue = numeric_limits<float>::max();
        }
        else if(_op == MAX){
            resultValue = numeric_limits<float>::min();
        }
        else{
            resultValue = 0.0;
        }

        
        for(auto v: group.second){
            readAttribute(allAttrs, aggAttr.name, v, attrValue, attrLength);
            float value;
            count++;
            if(aggAttr.type == TypeInt){
                int blob;
                memcpy(&blob, (char *)attrValue + sizeof(char), sizeof(int));
                value = (float)blob;
            }
            else if(aggAttr.type == TypeReal){
                memcpy(&value, (char *)attrValue + sizeof(char), sizeof(float));
            }
            if(_op == MIN){
                resultValue = min(resultValue, value);
            }
            else if(_op == MAX){
                resultValue = max(resultValue, value);
            }
            else if(_op == COUNT){
                resultValue = count;
            }
            else if (_op == SUM){
                resultValue += value;
            }
            else if(_op == AVG){
                resultValue += value;
            }

        }
        if(_op == AVG){
            resultValue /= count;
            
        }
        void* aggValue = malloc(PAGE_SIZE);
        char nullField = 0;
        nullField = ((count == 0) ? 1 : 0) << 7;
        memcpy((char *)aggValue, &nullField, sizeof(char));
        memcpy((char *)aggValue+sizeof(char), &resultValue, sizeof(float));
        int aggVLen = sizeof(int) + sizeof(char);

        void* aggGroup = malloc(PAGE_SIZE);
        int aggGLen;
        readAttribute(allAttrs, groupAttr.name, group.second.front(), aggGroup, aggGLen);
        
        void* combine = malloc(PAGE_SIZE);
        int combLen;
        
        combineTuple(aggGroup, {groupAttr}, aggGLen, aggValue, {aggAttr}, aggVLen, combine, combLen);
        void* out = malloc(combLen - sizeof(int));
        memcpy((char *)out, (char *)combine+sizeof(int), combLen - sizeof(int) );
        output.push_back({combLen-sizeof(int), out});

        // clear the group vector
        for(auto v: group.second){
            free(v);
        }
        group.second.clear();

    }
    outIt = output.begin();
    free(data);
    free(tuple);
    free(attrValue);



}

Aggregate::~Aggregate()
{
    for(int i =0; i < output.size(); i++){
        free(output[i].second);
    }
    output.clear();
}


RC Aggregate::getNextTuple(void* data)
{
    if(outIt != output.end()){

        memcpy((char *)data, (char *)outIt->second, outIt->first);
        outIt++;
        return 0;
    }
    else{
        return QE_EOF;
    }
}
void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
    attrs = this->outAttrs;

}


GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
)
{

    rm = RelationManager::instance();

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    _condition = condition;

    void* data = malloc(PAGE_SIZE), *tuple = malloc(PAGE_SIZE),*attrValue = malloc(PAGE_SIZE);
    int dataSize, tupleSize, attrLength;
    RID rid;

    Attribute rhsAtt, lhsAtt; 
    for(auto attr: leftAttrs){
        if(attr.name == _condition.lhsAttr){
            lhsAtt = attr;
        }
        joinAttrs.push_back(attr);
    }
    for(auto attr: rightAttrs){
        if(attr.name == _condition.rhsAttr){
            rhsAtt = attr;
        }
        joinAttrs.push_back(attr);
    }
    if(lhsAtt.type != rhsAtt.type){
        return;
    }

    compAttr = lhsAtt;

    leftPartName += "left_" + _condition.lhsAttr + "_join"; // left partition name
    rightPartName += "right_" + _condition.rhsAttr + "_join"; // right parition name

    for(int i =0; i < numPartitions ; i++){
        string fp = leftPartName + to_string(i);
        rm->createTable(fp, leftAttrs);
        fp = rightPartName + to_string(i);
        rm->createTable(fp, rightAttrs);
    }
    // cout << "processing left parition..." << endl;
    int cnt = 0;


    hash<int> hashInt;
    hash<float> hashFloat;
    hash<string> hashStr;
    string fileName;
    // write left parition to disk
    while(leftIn->getNextTuple(data) != QE_EOF){
        readAttribute(leftAttrs, _condition.lhsAttr, data, attrValue, attrLength);
        size_t hashValue = 0;
        if(compAttr.type   == TypeInt){
            int key;
            memcpy(&key, (char *)attrValue + sizeof(char), sizeof(int) );
            
            hashValue = hashInt(key);
        }
        else if(compAttr.type == TypeReal){
            float key;
            
            memcpy(&key, (char *)attrValue + sizeof(char), sizeof(float) );
            
            hashValue = hashFloat(key);
            
        }
        else if(compAttr.type == TypeVarChar){
            char* key = new char[attrLength - sizeof(char) - sizeof(int) +1];
            memcpy((char *)key, (char *)attrValue + sizeof(char) + sizeof(int) , attrLength -sizeof(char) - sizeof(int) );
            key[attrLength- sizeof(char) - sizeof(int)] = '\0';
            string strKey = key;
            // hash<char*> myhash;
            hashValue = hashStr(strKey);
            // cout << "str key: " << strKey << endl;
            delete [] key;
            
        }
        hashValue = hashValue%numPartitions;
       
        fileName = leftPartName + to_string(hashValue);
        rm->insertTuple(fileName, data, rid);
        cnt++;
        // cout << "insert tuple " << cnt << endl;
        
    }
    
    
    // write right partition to disk
    
    cnt = 0;
    
    while(rightIn->getNextTuple(data) != QE_EOF){
        readAttribute(rightAttrs, _condition.rhsAttr, data, attrValue, attrLength);
        size_t hashValue = 0;
        if(compAttr.type   == TypeInt){
            int key;
            memcpy(&key, (char *)attrValue + sizeof(char), sizeof(int) );
            // hash<int> myhash;
            hashValue = hashInt(key);
        }
        else if(compAttr.type == TypeReal){
            float key;
            memcpy(&key, (char *)attrValue + sizeof(char), sizeof(float) );
            // hash<float> myhash;
            hashValue = hashFloat(key);
            
        }
        else if(compAttr.type == TypeVarChar){
            char* key2 = new char[attrLength - sizeof(char) - sizeof(int)+1];
            memcpy((char *)key2, (char *)attrValue  + sizeof(char) + sizeof(int), attrLength- sizeof(char) - sizeof(int));
            // hash<char*> myhash;
            key2[attrLength- sizeof(char) - sizeof(int)] = '\0';
            string strKey = key2;
            hashValue = hashStr(strKey); 
            // cout << "str key: " << strKey << endl;
            delete [] key2;
        }
        
        hashValue = hashValue%numPartitions;
        
        fileName = rightPartName + to_string(hashValue);
        
        rm->insertTuple(fileName, data, rid);
        cnt++;
        // cout << "insert tuple " << cnt << endl;
        
        

    }
    outputName = _condition.lhsAttr + "_join_" + _condition.rhsAttr;
    rm->createTable(outputName, joinAttrs);

    

    
    // for each patition, read out the left parition into hash

    
    string leftFileName, rightFileName;
    
    

    
    for(int n_part = 0; n_part < numPartitions; n_part++){
        cout << "processing " << n_part << " parition..." << endl;
        leftFileName = leftPartName + to_string(n_part);
        rightFileName = rightPartName + to_string(n_part);
      
        TableScan *leftTs = new TableScan(*rm, leftFileName);
        TableScan *rightTs = new TableScan(*rm, rightFileName);
        unordered_map<int, vector<void*> > intMap;
        unordered_map<float, vector<void*> > floatMap;
        unordered_map<string, vector<void*> > strMap;
       
        while (leftTs->getNextTuple(data) != QE_EOF){
            
            interpretTuple(leftAttrs, data, tuple, tupleSize, dataSize);
            readAttribute(leftAttrs, _condition.lhsAttr, data, attrValue, attrLength);
            void* inData = malloc(tupleSize);
            memcpy((char *)inData, (char *)tuple, tupleSize);
            if (compAttr.type == TypeInt){
                int keyLeft;
                memcpy(&keyLeft, (char *)attrValue + sizeof(char), sizeof(int));
                intMap[keyLeft].push_back(inData);
            }
            else if (compAttr.type == TypeReal){
                float keyLeft;
                memcpy(&keyLeft, (char *)attrValue + sizeof(char), sizeof(float));
                floatMap[keyLeft].push_back(inData);
            }
            else if(compAttr.type == TypeVarChar){
                char* keyCharLeft = new char[attrLength - sizeof(char) - sizeof(int)+1];
                memcpy((char *)keyCharLeft, (char *)attrValue + sizeof(char) + sizeof(int), attrLength - sizeof(char) - sizeof(int));
                keyCharLeft[attrLength- sizeof(char) - sizeof(int)] = '\0';
                string keyLeft = keyCharLeft;
            
                strMap[keyLeft].push_back(inData);
                delete [] keyCharLeft;
            }    
        }
        void* rightTuple = malloc(PAGE_SIZE);
        while(rightTs->getNextTuple(data) != QE_EOF){

            
            interpretTuple(rightAttrs, data, rightTuple, tupleSize, dataSize);
            readAttribute(rightAttrs, _condition.rhsAttr, data, attrValue, attrLength);

            int intKey;
            float floatKey;
            string strKey;
            if (compAttr.type == TypeInt){
                
                memcpy(&intKey, (char *)attrValue + sizeof(char), sizeof(int));

                
            }
            else if (compAttr.type == TypeReal){
                
                memcpy(&floatKey, (char *)attrValue + sizeof(char), sizeof(float));
            
            }
            else if(compAttr.type == TypeVarChar){
                char* keyCharRight = new char[attrLength - sizeof(char) - sizeof(int)+1];
                memcpy((char *)keyCharRight, (char *)attrValue+ sizeof(int) + sizeof(char), attrLength - sizeof(char) - sizeof(int));
                keyCharRight[attrLength - sizeof(char) - sizeof(int)] = '\0';
                strKey = keyCharRight;
                delete [] keyCharRight;
            
                // mymap[keyCharLeft].push_back(tuple);
                
            }
            // char* keyChar = new char[attrLength+1];
            // memcpy((char *)keyChar, (char *)attrValue, attrLength);
            // keyChar[attrLength] = '\0';
            // string keyRight(keyChar);
            // delete [] keyChar;

            if (compAttr.type == TypeInt){
                if(intMap.find(intKey) != intMap.end()){
                    for(auto matched_value: intMap[intKey]){
                        void* combTuple = malloc(PAGE_SIZE);
                        void* lhsData = malloc(PAGE_SIZE);
                        void* rhsData = malloc(PAGE_SIZE);
                        int lhsSize=0, rhsSize=0;
                        int combineSize;
                        recordToTA(leftAttrs, matched_value, lhsData, lhsSize);
                        recordToTA(rightAttrs,rightTuple, rhsData, rhsSize);

                        combineTuple(lhsData, leftAttrs, lhsSize, rhsData, rightAttrs,rhsSize, combTuple, combineSize );
                        void* out = malloc(combineSize- sizeof(int));
                        memcpy((char *)out, (char *)combTuple + sizeof(int), combineSize-sizeof(int));
                        
                        rm->insertTuple(outputName, out, rid);
                        free(out);
                        free(combTuple);
                        free(lhsData);
                        free(rhsData);
                    }
                }
            }
            else if (compAttr.type == TypeReal){
                if(floatMap.find(floatKey) != floatMap.end()){
                    for(auto matched_value: floatMap[floatKey]){
                        void* combTuple = malloc(PAGE_SIZE);
                        void* lhsData = malloc(PAGE_SIZE);
                        void* rhsData = malloc(PAGE_SIZE);
                        int lhsSize=0, rhsSize=0;
                        int combineSize;
                        recordToTA(leftAttrs, matched_value, lhsData, lhsSize);
                        recordToTA(rightAttrs,rightTuple, rhsData, rhsSize);

                        combineTuple(lhsData, leftAttrs, lhsSize, rhsData, rightAttrs,rhsSize, combTuple, combineSize );
                        void* out = malloc(combineSize- sizeof(int));
                        memcpy((char *)out, (char *)combTuple + sizeof(int), combineSize-sizeof(int));
                        
                        rm->insertTuple(outputName, out, rid);
                        free(out);
                        free(combTuple);
                        free(lhsData);
                        free(rhsData);
                    }
                }
            }
            else if(compAttr.type == TypeVarChar){
                if(strMap.find(strKey) != strMap.end()){
                    for(auto matched_value: strMap[strKey]){
                        void* combTuple = malloc(PAGE_SIZE);
                        void* lhsData = malloc(PAGE_SIZE);
                        void* rhsData = malloc(PAGE_SIZE);
                        int lhsSize=0, rhsSize=0;
                        int combineSize;
                        recordToTA(leftAttrs, matched_value, lhsData, lhsSize);
                        recordToTA(rightAttrs,rightTuple, rhsData, rhsSize);

                        combineTuple(lhsData, leftAttrs, lhsSize, rhsData, rightAttrs,rhsSize, combTuple, combineSize );
                        void* out = malloc(combineSize- sizeof(int));
                        memcpy((char *)out, (char *)combTuple + sizeof(int), combineSize-sizeof(int));
                        
                        rm->insertTuple(outputName, out, rid);
                        free(out);
                        free(combTuple);
                        free(lhsData);
                        free(rhsData);
                    }
                }
            }
            
            // delete [] keyChar;
        }
        free(rightTuple);

        // // // clear my map

        if (compAttr.type == TypeInt){
            for(auto a: intMap){
                for( auto v:a.second){
                    free(v);
                }
                // delete [] a.first;
                a.second.clear();
            
            }
            intMap.clear();
        }
        else if(compAttr.type == TypeReal){
            for(auto a: floatMap){
                for( auto v:a.second){
                    free(v);
                }
                // delete [] a.first;
                a.second.clear();
            
            }
            floatMap.clear();

        }
        else if(compAttr.type == TypeVarChar){
            for(auto a: strMap){
                for( auto v:a.second){
                    free(v);
                }
                // delete [] a.first;
                a.second.clear();
            
            }
            strMap.clear();
        }
            
        

        delete rightTs;
        delete leftTs;

        rm->deleteTable(leftFileName);
        rm->deleteTable(rightFileName);
        


    }
    free(data);
    free(tuple);
    free(attrValue);


    outIt = new TableScan(*rm, outputName);




}
GHJoin::~GHJoin()
{
    delete outIt;
    rm->deleteTable(outputName);
    
}
RC GHJoin::getNextTuple(void *data)
{
    return outIt->getNextTuple(data);
}
void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs = this->joinAttrs;
}

// void GHJoin::match(int numParition, TableScan *leftTs, TableScan *rightTs, AttrType compType, RelationManager &rm)
// {
//     void* data = malloc(PAGE_SIZE), *tuple = malloc(PAGE_SIZE), *attrValue = malloc(PAGE_SIZE);
//     int dataSize, tupleSize, attrSize;


//     if(compType == TypeInt){
//         unordered_map<int, vector<void*> > mymap;
        
//     }

// }




