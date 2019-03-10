
#include "rm.h"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <string.h>
#include <sstream>

RelationManager* RelationManager::_rm_manager = 0;

RelationManager* RelationManager::instance()
{
//    static RelationManager _rm;
//    return &_rm;
    if(!_rm_manager)
    	_rm_manager = new RelationManager();

    return _rm_manager;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	ix = IndexManager::instance();
}


RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	RC rc;
	rc = rbfm->createFile("Tables");
	if(rc == -1) return rc;
	rc = rbfm->createFile("Columns");
	if(rc == -1) return rc;

	FileHandle fhTb, fhCl;
	rbfm->openFile("Tables", fhTb);
	if(rc == -1) return rc;
	rbfm->openFile("Columns", fhCl);
	if(rc == -1) return rc;

	createTableSlot(fhTb, 1, "Tables", "Tables");
	createTableSlot(fhTb, 2, "Columns", "Columns");
	createColumnSlot(fhCl, 1, "table-id", TypeInt, 4 , 1);
	createColumnSlot(fhCl, 1, "table-name", TypeVarChar, 50, 2);
	createColumnSlot(fhCl, 1, "file-name", TypeVarChar, 50, 3);
	createColumnSlot(fhCl, 2, "table-id", TypeInt, 4, 1);
	createColumnSlot(fhCl, 2, "column-name",  TypeVarChar, 50, 2);
	createColumnSlot(fhCl, 2, "column-type", TypeInt, 4, 3);
	createColumnSlot(fhCl, 2, "column-length", TypeInt, 4, 4);
	createColumnSlot(fhCl, 2, "column-position", TypeInt, 4, 5);

	rbfm->closeFile(fhTb);
	rbfm->closeFile(fhCl);
    return 0;
}

RC RelationManager::deleteCatalog()
{
	RC rc;
	rc = rbfm->destroyFile("Tables");
	if(rc == -1) return rc;
	rc = rbfm->destroyFile("Columns");
	if(rc == -1) return rc;
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	/*
	tables: interpret to (table-id, tableName, table_fileName)
	interpret table-id by pageNum*200 + slotNum;
	createTableSlot
	columns: interpret record attribute to catalog attribute
	createColumnSlot
	create new file by table_fileName
	*/


	// - tables
	string file_name = tableName;
	int record_size = 4 + 3 * 2 * sizeof(short) + 4 + tableName.size() + file_name.size();
	int next_record_pos = 0;
	FileHandle fhTb;
	rbfm->openFile("Tables", fhTb);

	// interpret table-id
	RID insertPlace = rbfm->findFreePages_Slot(fhTb, record_size + sizeof(int)*2, next_record_pos);

	// use table_id to create table slot
	int table_id = insertPlace.pageNum*200 + insertPlace.slotNum + 1;
	//cout << "table id is: " << table_id << endl;
	createTableSlot(fhTb, table_id, tableName, file_name);

	rbfm->closeFile(fhTb);


	// - columns
	FileHandle fhCl;
	rbfm->openFile("Columns", fhCl);
	for(int i = 1; i <= attrs.size(); ++i){
		createColumnSlot(fhCl, table_id, attrs[i-1].name, attrs[i-1].type, attrs[i-1].length, i);
	}
	rbfm->closeFile(fhCl);

	// - create new file
	rbfm->createFile(file_name);


	// - create index catalog (format: tableName_idx)
	string idxFile = tableName + "_idx";
	RC rc;
	rc = rbfm->createFile(idxFile);
	if(rc == -1) return rc;

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	/*
	find the slot table by tableName, get table-id and table_fileName
	scan the columns table, if table-id matches, delete record
	scan the tables table, if table-id matches, delete record and break
	*/

	if(tableName == "Tables" || tableName == "Columns") return -1;

	// delete file table
	string file_name = tableName;
	RC rc;
	rc = rbfm->destroyFile(file_name);
	if(rc == -1) return rc;

	// delete index catalog
	string idxFile = tableName + "_idx";
	rc = rbfm->destroyFile(idxFile);
	if(rc == -1) return rc;

	// initiate parameters to scan tables and columns
	int table_id = findIDbyTableName(tableName);

	RID rid;
	void *returnedData = malloc(PAGE_SIZE);
	RM_ScanIterator rm_ScanIterator;

	// scan the columns table and delete
	//cout << "Column Scan" << endl;
	scan("Columns", "table-id", EQ_OP, &table_id, {}, rm_ScanIterator); // we could assign null vector since we only need RID


	while(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){
		//cout << rid.pageNum << " " << rid.slotNum << endl;
		deleteTuple("Columns", rid);
		//cout << "delete" << endl;
	}
	

	// scan the tables table and delete
	scan("Tables", "table-id", EQ_OP, &table_id, {}, rm_ScanIterator);
	if(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){
		//cout << rid.pageNum << " " << rid.slotNum << endl;
		deleteTuple("Tables", rid);
	}

	free(returnedData);
	rm_ScanIterator.close();
	//cout << "#### FINISH DELETING TABLE: " << tableName << " ####" << endl << endl;

    return rc;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	/*
	### IMPORTANT : DON'T use this function while you want to get attribute of Tables and Columns,
	### USE getTableAttributes, getColumnAttributes instead!!!
	go to tables_table to extract table-id by tableName
	scan at column_tables by table-id to find column-name, column-type, column-length and form them into a vector<Attribute>
	*/
	int table_id;
	if(tableName == "Tables") table_id = 1;
	else if(tableName == "Columns") table_id = 2;
	else table_id = findIDbyTableName(tableName);
	findAttribyColumn(table_id, attrs);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(tableName == "Tables" || tableName == "Columns") return -1;
    // use tableName to find fileName
    // create filehandler to createfile by fileName
    // get Attribute

    RC rc;
    FileHandle fhTb;
    vector<Attribute> attrs;

    rc = rbfm->openFile(tableName, fhTb);
    if(rc == -1) return rc;

    rc = getAttributes(tableName, attrs);
    if(rc == -1) return rc;
    rc = rbfm->insertRecord(fhTb, attrs, data, rid);

    // insert entry at index file
	void* record = malloc(PAGE_SIZE);
	int recordSize;
	rbfm->interpretRecord(attrs, data, record, &recordSize);
    formEntryAndExec(tableName, attrs, rid, record, Insert);
    free(record);

    if(rc == -1) return rc;

    rc = rbfm->closeFile(fhTb);
    if(rc == -1) return rc;

    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	//if(tableName == "Tables" || tableName == "Columns") return -1;
    // get attributes with getAttributes function
    vector<Attribute> attr;

	RC rc;
    rc = getAttributes(tableName, attr);
	if (rc == -1) return -1;
    
	// create the fileHandle and open file with this filehandle
	FileHandle fhTb;
    rc = rbfm->openFile(tableName, fhTb);
	if (rc == -1) return -1;


	// delete entry at index file
	void* data = malloc(PAGE_SIZE);
	readTuple(tableName, rid, data);
	void* record = malloc(PAGE_SIZE);

	int recordSize;
	rbfm->interpretRecord(attr, data, record, &recordSize);

	if(tableName != "Columns" && tableName != "Tables")
		formEntryAndExec(tableName, attr, rid, record, Delete);

	free(data);
	free(record);

	// call deleteRecord
    rc = rbfm->deleteRecord(fhTb, attr, rid);
	if (rc == -1) return rc;
	rc = rbfm->closeFile(fhTb);
	if(rc == -1) return rc;


    return 0;
    
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(tableName == "Tables" || tableName == "Columns") return -1;
    // get attributes with getAttributes function
    // create the fileHandle and open file with this filehandle
    // call updateRecord
    vector<Attribute> attr;
	FileHandle fhTb;
    RC rc;
    rc = getAttributes(tableName, attr);
	if (rc == -1) return rc;
    
    rc = rbfm->openFile(tableName, fhTb);
    if(rc == -1) return rc;

    // delete old entry from index
	void* olddata = malloc(PAGE_SIZE);
	readTuple(tableName, rid, olddata);
	void* record = malloc(PAGE_SIZE);

	int recordSize;
	rbfm->interpretRecord(attr, olddata, record, &recordSize);
	formEntryAndExec(tableName, attr, rid, record, Delete);

	free(olddata);
	free(record);

	// update data
    rc = rbfm->updateRecord(fhTb, attr, data, rid);

    // insert new entry into index
	record = malloc(PAGE_SIZE);
	rbfm->interpretRecord(attr, data, record, &recordSize);
    formEntryAndExec(tableName, attr, rid, record, Insert);
    free(record);

    if(rc == -1) return rc;

	rc = rbfm->closeFile(fhTb);
	if(rc == -1) return rc;
       
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	// use tableName to find fileName
	// create filehandler to createfile by fileName
	// get Attribute
	RC rc;
	FileHandle fhTb;
	vector<Attribute> attrs;

	rc = rbfm->openFile(tableName, fhTb);
	if(rc == -1) return rc;

	rc = getAttributes(tableName, attrs);
	if(rc == -1) return rc;
	//cout << "REAL read Record=========" << endl;
	rc = rbfm->readRecord(fhTb, attrs, rid, data);
	if(rc == -1) return rc;

	rc = rbfm->closeFile(fhTb);
	if(rc == -1) return rc;

    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	rbfm->printRecord(attrs, data);
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> attr;
	FileHandle fhTb;
	RC rc;
    rc = getAttributes(tableName, attr);
	if (rc == -1) return rc;
    
    rc = rbfm->openFile(tableName, fhTb);
	if (rc == -1) return rc;
    rc = rbfm->readAttribute(fhTb, attr, rid, attributeName, data);
    if(rc == -1) return rc;
	rc = rbfm->closeFile(fhTb);
	if(rc == -1) return rc;

    return 0;
    
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	// use tableName to find fileName
	// create filehandler to createfile by fileName
	vector<Attribute> attrs;
	RC rc;
	if(tableName == "Tables") getTableAttributes(attrs);
	else if(tableName == "Columns") getColumnAttributes(attrs);
	else if(tableName.substr(tableName.size()-4, 4) == "_idx") getIndexAttributes(attrs); // index file
	else getAttributes(tableName, attrs);
	FileHandle fh;

	rc = rbfm->openFile(tableName, fh);
	//if(rc == -1) cout << "openFile Fail" << endl;
	if(rc ==  -1) return rc;
	rc = rbfm->scan(fh, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
	//if(rc == -1) cout << "scan file at rbfm scan" << endl;
	if(rc ==  -1) return rc;

	// LOOK OUT
	//rc = rbfm->closeFile(fh);
    return 0;
}


// ######################################################
// there are 2 kinds of files: index catalog & index file
// index catalog (format: tableName + "_idx"): records index file name and attribute name (which represent key at index file)
// index file (format: tableName + "_idx_" + attributeName): the file we create at project 3

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	string idx_catalog_name = tableName + "_idx";
	string idx_file_name = tableName + "_idx_" + attributeName;

	// create an index file
	RC rc;
	rc = ix->createFile(idx_file_name);
	if(rc == -1) return rc;
	IXFileHandle ixFh;
	ix->openFile(idx_file_name, ixFh);
	if(rc == -1) return rc;
	if(rc == -1) return rc;
	ix->closeFile(ixFh);

	// insert slot at index catalog file
	FileHandle fhIdx;
	rbfm->openFile(idx_catalog_name, fhIdx);
	createIndexSlot(fhIdx, idx_file_name, attributeName);
	rbfm->closeFile(fhIdx);

	// initiate index with data
	initIdxwithData(tableName, attributeName);

	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string idx_catalog_name = tableName + "_idx";
	string idx_file_name = tableName + "_idx_" + attributeName;

	// delete index file
	RC rc;
	rc = ix->destroyFile(idx_file_name);
	if(rc == -1) return rc;

	// scan index catalog to find the slot whose attr_name = attributeName
	RID rid;
	void *returnedData = malloc(200);

	int attrLength = attributeName.size();
	void* attr_name = malloc(attrLength + sizeof(int));
	memcpy(attr_name, &attrLength, sizeof(int));

	const char* tNcr = attributeName.c_str();
	for(int i = 0; i < attrLength; ++i){
		memcpy((char*)attr_name + sizeof(int) + i, (char*)tNcr + i, sizeof(char));
	}

	RM_ScanIterator rm_ScanIterator;
	scan(idx_catalog_name, "attribute-name", EQ_OP, (char *)attr_name, {}, rm_ScanIterator); // need to put charLength in front of slot

	if(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){
		//cout << rid.pageNum << " " << rid.slotNum << endl;
		deleteTuple(idx_catalog_name, rid);
	}

	rm_ScanIterator.close();

	free(attr_name);
	free(returnedData);
    return rc;
}



RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	RC rc;
	IXFileHandle ixFh;
	string idx_file_name = tableName + "_idx_" + attributeName;

	// find attribute by attributeName
	vector<Attribute> attrs;
	Attribute attr;
	getAttributes(tableName, attrs);
	for(int i = 0; i < attrs.size(); ++i){
		if(attrs[i].name == attributeName){
			attr = attrs[i];
			break;
		}
	}

	rc = ix->openFile(idx_file_name, ixFh);
	if(rc ==  -1) return rc;

	rc = ix->scan(ixFh, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
	if(rc ==  -1) return rc;
	return rc;
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	RC rc = rbfm_ScanIterator.getNextRecord(rid, data);
	return rc;
}


RC RM_ScanIterator::close() {
	RC rc = rbfm_ScanIterator.close();
	return rc;
}


RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
	RC rc = ix_ScanIterator.getNextEntry(rid, key);
	return rc;
}
RC RM_IndexScanIterator::close(){
	RC rc = ix_ScanIterator.close();
	return rc;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

int RelationManager::findIDbyTableName(const string &tableName){
	int tableID;
    RID rid;
    RC rc;
    void *returnedData = malloc(200);
	RM_ScanIterator rm_ScanIterator;

	// add length in the first 4 bytes of value;
	int nameLength = tableName.size();
	void* name = malloc(nameLength + 4);
	memcpy(name, &nameLength, sizeof(int));

	const char* tNcr = tableName.c_str();
	for(int i = 0; i < nameLength; ++i){
		memcpy((char*)name + 4 + i, (char*)tNcr + i, sizeof(char));
	}

	rc = scan("Tables", "table-name", EQ_OP, name, {"table-id"}, rm_ScanIterator);
	//if(rc == -1) cout << "SCAN FAILED" << endl;
    if(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){
        // Null field length
        void* nullField = malloc(1);
        memcpy(nullField, returnedData, 1);
        vector<bool> isNull;
        rbfm->getNullVector(1, nullField, isNull);

        if(isNull[0]) tableID = -1;
		
        else memcpy(&tableID, (char *)returnedData+1, sizeof(int));
        /*
        if(tableID == -1){
        	cout << rid.pageNum << " " << rid.slotNum << endl;
        	cout << "The table ID is invalid" << endl << endl;
        }*/

        free(nullField);
	}
    else{
    	//cout << "RM_EOF! unable to find" << endl;
    	tableID = -1;
    }

	rm_ScanIterator.close();
	free(name);
	free(returnedData);
	return tableID;
}

void RelationManager::findAttribyColumn(int& table_id, vector<Attribute>& attrs){
	//cout << " Start to find Attribute by Column." << endl;

    RID rid;
    void *returnedData = malloc(200);
	RM_ScanIterator rm_ScanIterator;

	//cout << "matching table id is: " << table_id << endl;
	scan("Columns", "table-id", EQ_OP, &table_id, {"column-name", "column-type", "column-length"}, rm_ScanIterator);
	while(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){
		// fetch one tuple at at time and process the data;
		//cout << "matching columd rid is: " << rid.pageNum << " " << rid.slotNum << endl;
		Attribute attr;
		int offset = 1;

        // Null field length
        void* nullField = malloc(1);
        memcpy(nullField, returnedData, 1);
        vector<bool> isNull;
        rbfm->getNullVector(1, nullField, isNull);

        // build attribute
        // column-name
		if(isNull[0]) attr.name = "NULL";
		else{
			int nameLength;
			memcpy(&nameLength, (char *)returnedData + sizeof(char), sizeof(int));
			offset += sizeof(int);

			char* col_name = new char[nameLength]();
			memcpy(col_name, (char *)returnedData + offset, nameLength);
			offset += nameLength;
			for(int j = 0; j < nameLength; ++j) attr.name += col_name[j];
			delete [] col_name;
		}
		// column-type
		if(isNull[1]) attr.type = TypeInt;
		else{
			AttrType col_type;
			memcpy(&col_type, (char *)returnedData + offset, sizeof(int));
			offset += sizeof(int);
			attr.type = col_type;
			//cout << "Type is: " << col_type << endl;
		}
		// column-length
		if(isNull[2]) attr.length = -1;
		else{
			int col_length;
			memcpy(&col_length, (char *)returnedData + offset, sizeof(int));
			offset += sizeof(int);
			attr.length = col_length;
			//cout << "Length is: " << col_length << endl;
		}

		attrs.push_back(attr);
		free(nullField);
	}
	rm_ScanIterator.close();
	free(returnedData);
}

void RelationManager::getTableAttributes(vector<Attribute> &attrs){
	Attribute attr;

	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);
	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = 50;
	attrs.push_back(attr);
	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = 50;
	attrs.push_back(attr);
}

void RelationManager::getColumnAttributes(vector<Attribute> &attrs){
	Attribute attr;

	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);
	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = 50;
	attrs.push_back(attr);
	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);
	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);
	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = 4;
	attrs.push_back(attr);
}

void RelationManager::getIndexAttributes(vector<Attribute> &attrs){
	Attribute attr;

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = 50;
	attrs.push_back(attr);
	attr.name = "attribute-name";
	attr.type = TypeVarChar;
	attr.length = 50;
	attrs.push_back(attr);
}

void RelationManager::formEntryAndExec(const string &tableName, vector<Attribute> &attrs, const RID &rid, const void* record, EntryOp eOp){

	string idx_catalog_name = tableName + "_idx";
	//string idx_file_name = tableName + "_idx_" + attributeName;
	int cnt = 0;
	// debug
	int numField;
	memcpy(&numField, (char *)record, sizeof(int));
	int mycount = 0;
	//assert(numField == attrs.size());
	
	for(auto& attr : attrs){
		RID iterRid;
		void *returnedData = malloc(PAGE_SIZE);
		RM_ScanIterator rm_ScanIterator;

		// ## extract data slot by attr ##

		// extract offset and length
		int offsetPos = sizeof(int) + cnt * 2 * sizeof(short);
		int LengthPos = sizeof(int) + cnt * 2 * sizeof(short) + sizeof(short);
		short offset, length;
		memcpy(&offset, (char *)record + offsetPos, sizeof(short));
		memcpy(&length, (char *)record + LengthPos, sizeof(short));
		++cnt;
		if(length == 0) 
		{
			// cout << attr.name << " is null, skip." << endl;
			free(returnedData);
			continue; // null
		}

		// extract attribute of slot (not sure)
		void* slot;
		if(attr.type != TypeVarChar){
			slot = malloc(length);
			memcpy((char *)slot, (char *)record + offset, length);
		}
		else{
			int l = (int)length;
			slot = malloc(sizeof(int) + length);
			memcpy((char *)slot, &l, sizeof(int));
			memcpy((char *)slot + sizeof(int), (char *)record + offset, length);
		}

		// prepare input of scan
		vector<string> wantedAttr = {"file-name"};

		int attrLength = attr.name.size();
		void* attr_name = malloc(attrLength + sizeof(int));
		memcpy(attr_name, &attrLength, sizeof(int));

		const char* tNcr = attr.name.c_str();
		for(int i = 0; i < attrLength; ++i){
			memcpy((char*)attr_name + sizeof(int) + i, (char*)tNcr + i, sizeof(char));
		}

		// scan the tables table and delete
		scan(idx_catalog_name, "attribute-name", EQ_OP, (char *)attr_name, wantedAttr, rm_ScanIterator); // need to put charLength in front of slot
		if(rm_ScanIterator.getNextTuple(iterRid, returnedData) != RM_EOF){
			//cout << "iterRID is: " << iterRid.pageNum <<  ' ' << iterRid.slotNum << endl;
			// openFile by fileName

	        // Null field length
	        void* nullField = malloc(sizeof(char));
	        memcpy(nullField, returnedData, sizeof(char));
	        vector<bool> isNull;
	        rbfm->getNullVector(sizeof(char), nullField, isNull);

	        // debug
	        if(isNull[0]){
	        	cout << "RETURNDATA FAIL!" << endl;
	        	return;
	        }

			int l;
			memcpy(&l, (char*) returnedData + sizeof(char), sizeof(int));
			char* fN = new char[l+1];
			memcpy(fN, (char *)returnedData + sizeof(char) + sizeof(int), l);
			fN[l] = '\0'; //  THESE ONE IS IMPORTANT !!!!!
			string fileName(fN);
			// cout << fileName << endl;
			IXFileHandle ixFh;
			ix->openFile(fileName, ixFh);

			if(eOp == Insert) {
				ix->insertEntry(ixFh, attr, slot, rid);
				
				// ix->printBtree(ixFh, attr);
			}
			else ix->deleteEntry(ixFh, attr, slot, rid);
			// RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

			ix->closeFile(ixFh);
			delete [] fN;
			free(nullField);
		}
		rm_ScanIterator.close();

		
		free(slot);
		free(attr_name);
		free(returnedData);
	}
}

void RelationManager::initIdxwithData(const string &tableName, const string &attributeName){

	// init
	string idx_file_name = tableName + "_idx_" + attributeName;
	Attribute attr;
	IXFileHandle ixFh;
	ix->openFile(idx_file_name, ixFh);

	 // extract attribute
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	for(int i = 0; i < attrs.size(); ++i){
		if(attrs[i].name == attributeName){
			attr = attrs[i];
			break;
		}
	}

	// scan table(all slots)
	RID rid;
	void *returnedData = malloc(PAGE_SIZE);
	RM_ScanIterator rm_ScanIterator;
	vector<string> attributesStr;
	attributesStr.push_back(attributeName);

	RC rc = scan(tableName, "", NO_OP, NULL, attributesStr, rm_ScanIterator);
	if(rc == -1) return;

	while(rm_ScanIterator.getNextTuple(rid, returnedData) != RM_EOF){
		// cout << rid.pageNum << ' ' << rid.slotNum << endl;

		// extract part of slots by attribute name
         // Null field length
        void* nullField = malloc(sizeof(char));
        memcpy((char *)nullField, returnedData, sizeof(char));
        vector<bool> isNull;
        rbfm->getNullVector(sizeof(char), nullField, isNull);
        if(isNull[0]) {

			continue;
		}

         // convert slot
        int l = 0;
        if(attr.type == TypeVarChar) memcpy(&l, (char*) returnedData + sizeof(char), sizeof(int));
        void* slot = malloc(sizeof(int)+l);
        memcpy((char*)slot, (char*)returnedData + sizeof(char), sizeof(int)+l);
		// cout << "insert " << *(int *)slot << endl;
        // insert
        ix->insertEntry(ixFh, attr, slot, rid);

        free(slot);
        free(nullField);
	}
	cout << "RM_EOF" << endl;


	free(returnedData);
	ix->closeFile(ixFh);
	rm_ScanIterator.close();
}


void RelationManager::createTableSlot(FileHandle &fh, int table_id, const string table_name, const string file_name){
	void* data = malloc(PAGE_SIZE);
	int data_offset = 0, field_sz;

	// insert number of field
	int fieldNum = 3;

	memcpy((char *)data + data_offset, &fieldNum, sizeof(int));
	data_offset += sizeof(int);

	// setting 2 offset : (1)offset of record directory  (2) data offset
	int record_dir_offset = data_offset;
	data_offset += fieldNum * sizeof(short) * 2;

	// insert table_id (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = 4;
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, &table_id, field_sz);
	data_offset += field_sz;

	// insert table_name (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = table_name.size();
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, (char *)table_name.c_str(), field_sz);
	data_offset += field_sz;

	// insert file_name (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = file_name.size();
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char* )data + data_offset, (char *)file_name.c_str(), field_sz);
	data_offset += field_sz;

	// find place to insert at table page and insert by RID
	int next_record_pos = 0;
	//FileHandle fh;
	//rbfm->openFile("Tables", fh);
	RID insertPlace = rbfm->findFreePages_Slot(fh, data_offset + sizeof(int)*2, next_record_pos);
	//cout << "insert table slot at page: " << insertPlace.pageNum << " slot:" << insertPlace.slotNum << ", pos: " << next_record_pos << ", length: " << data_offset << endl;
	rbfm->insertByRID(fh, insertPlace, next_record_pos, data_offset, data);

	//rbfm->closeFile(fh);

	free(data);
}

void RelationManager::createColumnSlot(FileHandle &fh, int table_id, const string column_name, int column_type, int column_length, int column_position){
	void* data = malloc(PAGE_SIZE);
	int data_offset = 0, field_sz;;

	// insert number of field
	int fieldNum = 5;
	memcpy((char *)data + data_offset, &fieldNum, sizeof(int));
	data_offset += sizeof(int);

	// setting 2 offset : (1)offset of record directory  (2) data offset
	int record_dir_offset = data_offset;
	data_offset += fieldNum * sizeof(short) * 2;

	// insert table_id (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = 4;
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, &table_id, field_sz);
	data_offset += field_sz;

	// insert column_name (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = column_name.size();
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, (char *)column_name.c_str(), field_sz);
	data_offset += field_sz;

	// insert column_type (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = 4;
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, &column_type, field_sz);
	data_offset += field_sz;

	// insert column_length (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = 4;
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, &column_length, field_sz);
	data_offset += field_sz;

	// insert column_position (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = 4;
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char *)data + data_offset, &column_position, field_sz);
	data_offset += field_sz;

	// find place to insert at table page and insert by RID
	int next_record_pos = 0;
	//FileHandle fh;
	//rbfm->openFile("Columns", fh);
	RID insertPlace = rbfm->findFreePages_Slot(fh, data_offset + sizeof(int)*2, next_record_pos);
	//cout << "insert columns slot at page: " << insertPlace.pageNum << " slot:" << insertPlace.slotNum << ", pos: " << next_record_pos << ", length: " << data_offset << endl;
	rbfm->insertByRID(fh, insertPlace, next_record_pos, data_offset, data);
	//rbfm->closeFile(fh);

	free(data);
}


void RelationManager::createIndexSlot(FileHandle &fh, const string file_name, const string attribute_name){
	void* data = malloc(PAGE_SIZE);
	int data_offset = 0, field_sz;

	// insert number of field
	int fieldNum = 2;

	memcpy((char *)data + data_offset, &fieldNum, sizeof(int));
	data_offset += sizeof(int);

	// setting 2 offset : (1)offset of record directory  (2) data offset
	int record_dir_offset = data_offset;
	data_offset += fieldNum * sizeof(short) * 2;

	// insert file_name (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = file_name.size();
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char* )data + data_offset, (char *)file_name.c_str(), field_sz);
	data_offset += field_sz;

	// insert attribute_name (offset, length, data)
	memcpy((char *)data + record_dir_offset, &data_offset, sizeof(short));
	record_dir_offset += sizeof(short);
	field_sz = attribute_name.size();
	memcpy((char *)data + record_dir_offset, &field_sz, sizeof(short));
	record_dir_offset += sizeof(short);
	memcpy((char* )data + data_offset, (char *)attribute_name.c_str(), field_sz);
	data_offset += field_sz;


	// find place to insert at table page and insert by RID
	int next_record_pos = 0;
	//FileHandle fh;
	//rbfm->openFile("Tables", fh);
	RID insertPlace = rbfm->findFreePages_Slot(fh, data_offset + sizeof(int)*2, next_record_pos);
	//cout << "insert table slot at page: " << insertPlace.pageNum << " slot:" << insertPlace.slotNum << ", pos: " << next_record_pos << ", length: " << data_offset << endl;
	rbfm->insertByRID(fh, insertPlace, next_record_pos, data_offset, data);

	//rbfm->closeFile(fh);

	free(data);
}
