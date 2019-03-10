
#include "ix.h"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <string.h>
#include <sstream>



IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	rbfm = RecordBasedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    FILE *pFile;

    pFile = fopen(fileName.c_str(),"r");
    if (pFile != NULL){ //exists
        fclose(pFile);
        return -1;
    }

    pFile = fopen(fileName.c_str(), "wb");
    // create header page with all 0
    void* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);


    if (pFile != NULL){
    	// schema of header
    	// | ixReadPageCounter | ixWritePageCounter | ixAppendPageCounter | .. (deleted page id)... | record size | free space |
        int free_space = PAGE_SIZE - 3 * sizeof(int) - 2 * sizeof(int);
        memcpy((char *)page + PAGE_SIZE - sizeof(int), &free_space, sizeof(int));
        fwrite(page, sizeof(char), PAGE_SIZE, pFile);
		
        fclose(pFile);
        free(page);

        return 0;

    }else{
        free(page);
        return -1;
    }
}

RC IndexManager::destroyFile(const string &fileName)
{
    if (FILE * pfile = fopen(fileName.c_str(), "r")){
        fclose(pfile);

        // delete it
        if (remove(fileName.c_str()) == 0) return 0;
        else return -2;
    }
    else return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    FILE *pFile;
    pFile = fopen(fileName.c_str(), "r+");

    if (pFile != NULL){
        // check if the fileHandle already handle some file
        if (ixfileHandle.pfile != NULL){
            return -1;
        }
        // point to end of the file
        fseek(pFile, 0, SEEK_END);
        ixfileHandle.pfile = pFile;
        ixfileHandle.readPC();

        // create root index page
        if(ixfileHandle.getNumberOfPages() == 0){
        	void *page = malloc(PAGE_SIZE);
        	ixfileHandle.createEmptyPage(page, false, -1);
        	ixfileHandle.appendPage(page);
        	free(page);
        }
        return 0;
    }
    else return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    fseek(ixfileHandle.pfile, 0, SEEK_SET);
    ixfileHandle.writePC();

    ixfileHandle.ixReadPageCounter = 0;
    ixfileHandle.ixWritePageCounter = 0;
    ixfileHandle.ixAppendPageCounter = 0;
    fclose(ixfileHandle.pfile);
    ixfileHandle.pfile = NULL;
    return 0;
}

// TODO: if root is full
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	// ### WE SHOULD transfer the format of key FIRST
	int key_size = 4;
	if(attribute.type == TypeVarChar){
		int strleng;
		memcpy(&strleng, key, sizeof(int));
		key_size = strleng + sizeof(int);
	}
	initSlotSize(key_size);

	// init input
	void * nKey = malloc(KEY_SIZE);
	memcpy((char *)nKey, key, key_size);
	RID_ATTR ridPage = rid.pageNum;
	RID_ATTR ridSlot = rid.slotNum;
	memcpy((char *)nKey+key_size, &ridPage, sizeof(RID_ATTR));
	memcpy((char *)nKey+key_size+sizeof(RID_ATTR), &ridSlot, sizeof(RID_ATTR));

	void * page = malloc(PAGE_SIZE);
	ixfileHandle.readPage(0, page);

	int splitPage = -1; // if splitPage != -1, split happened
	dfs_insert(ixfileHandle, nKey, page, attribute, splitPage, 0);

	// if (splitPage != -1){
	// 	cout << "modified root :" <<  splitPage <<endl;
	// 	return 0;
	// 	void* split = malloc(PAGE_SIZE);
	// 	ixfileHandle.readPage(splitPage, split);
	// 	int newPageNum;
	// 	ixfileHandle.createFirstData(split, newPageNum);
	// 	void* newPage = malloc(PAGE_SIZE);
	// 	ixfileHandle.readPage(newPageNum, newPage);

	// 	int keyLeng = 0;
	// 	void *indexSlot = formIndexSlot(ixfileHandle, attribute, splitPage, keyLeng);
	// 	cout << "---------------------" << "PRINT STRING" << "-------------------------" << endl;
	// 	ixfileHandle.printStr(indexSlot, attribute);

	// 	// debug
	// 	if(ixfileHandle.getNumberOfPages() - 1 > 3){
	// 		void * tpg = malloc(PAGE_SIZE);
	// 		ixfileHandle.readPage(3, tpg);
	// 		int l = 0;
	// 		void *sl = ixfileHandle.extractIthSlot(1, tpg, l);
	// 		cout << "slot length is: " << l << endl;
	// 		ixfileHandle.printStr(sl, attribute);
	// 		free(sl);
	// 		free(tpg);
	// 	}

	// 	KEY_SIZE = keyLeng;
	// 	// insert the spliting point 

	// 	simpleInsert(ixfileHandle, indexSlot, newPage, splitPage, newPageNum, 1, false);

	// 	// swap the root page with the new page;
	// 	void* rootPage = malloc(PAGE_SIZE);
	// 	ixfileHandle.readPage(0, rootPage);
		
	// 	ixfileHandle.writePage(0, newPage);
	// 	ixfileHandle.writePage(newPageNum, rootPage);
	// 	free(rootPage);
	// 	free(split);
	// 	free(newPage);
		
		

		
	// }
	//else
    ixfileHandle.writePage(0, page);

    free(nKey);
	free(page);
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	// ### WE SHOULD transfer the format of key FIRST
	int key_size = 4;
	if(attribute.type == TypeVarChar){
		int strleng;
		memcpy(&strleng, key, sizeof(int));
		key_size = strleng + sizeof(int);
	}
	initSlotSize(key_size);

	// init input
	void * nKey = malloc(KEY_SIZE);
	memcpy((char *)nKey, key, key_size);
	int ridPage = rid.pageNum;
	int ridSlot = rid.slotNum;
	memcpy((char *)nKey+key_size, &ridPage, sizeof(int));
	memcpy((char *)nKey+key_size+sizeof(int), &ridSlot, sizeof(int));

	void * page = malloc(PAGE_SIZE);
	ixfileHandle.readPage(0, page);
	void* oldChildEntryKey = malloc(key_size);
	pair<void* , int> oldChildEntry = {oldChildEntryKey, -1};
	RC rc;
    rc = dfs_delete(ixfileHandle, nKey, page, attribute, oldChildEntry, 0, NULL, -1);
	//printBtree(ixfileHandle, attribute);
	//cout << "last rc" << rc << endl;
	if(rc != 0) {
		//cout << "error!" << endl;
		//printBtree(ixfileHandle, attribute);
		return -1;
	}

	//cout << "yeeeee" << endl;
    return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if(!ixfileHandle.pfile) return -1;
	ix_ScanIterator.init(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
	RID rid = ix_ScanIterator.findLowBound();
	ix_ScanIterator.setRID(rid);
	return 0;

}

string operator*(const string& s, unsigned int n) {
    stringstream out;
    while (n--)
        out << s;
    return out.str();
}

string operator*(unsigned int n, const string& s) { return s * n; }

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	preOrderTraversal(ixfileHandle, attribute, 0, 1);
}

void IndexManager::preOrderTraversal(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, int depth ) const{
	//if (depth >= 2) return;
	void* page = malloc(PAGE_SIZE);
	ixfileHandle.readPage(pageNum, page);
	//cout << "readpage" << endl;
	int p;
	memcpy(&p, (char *)page+9, sizeof(int));
	// cout << "1st element: " << p << endl;

	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
	
	unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char));
	bool isDataEntry = (isDataInt == 1 ? true : false);

	
	// extract keys and children
	vector<pair<void*, int> > keys;
	vector<vector<int> > children;
	
	if (!isDataEntry){
		int firstPointer;
		memcpy(&firstPointer, (char *)page+9, sizeof(int));
		children.push_back({firstPointer});
		// cout << "firstpointer: " <<firstPointer << endl;
	}
	// cout << "page num: " << pageNum << "record size " << recordSize << endl;
	for(int i = 1; i <= recordSize; i++){
		void* key = NULL;
		int keyLen;
		
		key = ixfileHandle.extractIthSlot(i, page, keyLen);
		// cout << "extract " << i << "slot" << endl;
		keys.push_back({key, keyLen});
		
		if (!isDataEntry){
			int pointer;
			memcpy(&pointer, (char *)key+keyLen - sizeof(int), sizeof(int));
			//cout << "pointers: " << pointer << endl;
			children.push_back({pointer});
		}
		else{
			int pageId, slotId;
			memcpy(&pageId, (char *)key+keyLen - sizeof(int)*2, sizeof(int));
			memcpy(&slotId, (char *)key+keyLen - sizeof(int)*1, sizeof(int));
			vector<int> rid;
			rid.push_back((int)pageId);
			rid.push_back((int)slotId);
			children.push_back(rid);
			
				
		}
		//free(key);
		
		
	} 
	//cout << "after collect" << endl;
	// if isDataEntry, make the key entry group by key value 
	

	if(isDataEntry && keys.size() != 0){
		vector<pair<void*, int> > tempKeys;
		vector<vector<int> > tempChildren;
		tempKeys.push_back(keys[0]);
		tempChildren.push_back(children[0]);
		for(int i = 1, j= 1; i < children.size() && j < keys.size();i++, j++){

			if(compare(keys[i].first, tempKeys.back().first, attribute,true) == 0){
				tempChildren.back().push_back(children[i][0]);
				tempChildren.back().push_back(children[i][1]);
			}
			else{
				tempKeys.push_back(keys[i]);
				tempChildren.push_back({children[i][0], children[i][1]});

			}

		}
		keys = tempKeys;
		children = tempChildren;
	}
	//cout << "after group" << endl;

	// print indent by Depth
	string indent = "    ";
	
	cout << indent*(depth-1) << "{";
	
	cout << "\"keys\":";
	cout << "[";
	
	vector<pair<void*, int> >::iterator key_it;
	int i;
	
	for(key_it = keys.begin(), i = 0; key_it != keys.end(); key_it++,i++){
		if(!isDataEntry){
			cout << "\"" ;
			if (attribute.type == TypeVarChar){
				int l;
				memcpy(&l, (char *)key_it->first, sizeof(int));
				char* key_value = new char[l] ;
				memcpy((char *)key_value, (char *)key_it->first + sizeof(int), l);
				//cout << "\"" << key_value << "\"";
				//cout << "\"";
				
				for(int j = 0; j < 2; j++) cout << key_value[j];

				cout << "+" <<l;
				//cout << "\"";

				delete [] key_value;
			}
			else if (attribute.type == TypeInt){
				int key_value;
				memcpy(&key_value, (char *)key_it->first, sizeof(int));
				//cout << "\"" << key_value << "\"";
			}
			else{
				float key_value;
				memcpy(&key_value, (char *)key_it->first, sizeof(float));
				//cout << "\"" << key_value << "\"";
			}
			cout << "\"" ;

			if (i != keys.size()-1)
				cout << ",";
		}
		else{
			cout << "\"" ;
			if (attribute.type == TypeVarChar){
				//int l = key_it->second - sizeof(int);
				int l;
				memcpy(&l, (char *)key_it->first, sizeof(int));
				char* key_value = new char[l] ;
				memcpy((char *)key_value, (char *)key_it->first + sizeof(int), l);
				
				for(int k = 0; k < 2; k++) cout << key_value[k];
				cout << l;
				//cout << "\"";
				delete [] key_value;
				
			}
			else if (attribute.type == TypeInt){
				int key_value;
				memcpy(&key_value, (char *)key_it->first, sizeof(int));
				cout << key_value;
			}
			else{
				float key_value;
				memcpy(&key_value, (char *)key_it->first, sizeof(float));
				cout << key_value;
			}
			cout << ":[";
			for(int j = 0; j < children[i].size(); j += 2){
				int pageId, slotId;
				pageId = children[i][j];
				slotId = children[i][j+1];
				
				
				cout  << "(" << pageId << "," << slotId << ")";
				if (j < children[i].size()-2)
					cout << ",";
			}
			cout << "]";
			cout << "\"" ;
			if( i != keys.size()-1)
				cout << ",";
		}
	}
	if (isDataEntry){
		cout << "]}," << endl;
	}else{
		cout << "]," << endl;
	}

	
	// print children
	if (!isDataEntry){
		string indent = "    ";
		cout << indent*(depth-1);
		//for(int d = 0; d < depth; d++) cout << " ";
		
		cout << "\"children\":[" << endl;
		
		for(int j = 0; j < children.size(); j++){
			cout << "pointer: " << children[j][0] << endl; ;
			preOrderTraversal(ixfileHandle, attribute, children[j][0], depth+1);
			
			
		}
		cout << indent*(depth-1);
		cout << "]}," << endl;
			
		
	}
	
	free(page);

}
int IndexManager::naiveBinarySearch(IXFileHandle &ixfileHandle, const void *key, void* page, const Attribute &attribute, bool isDataEntry, bool scanBS)
{
	// this function would return the smallest equal entry
	// if the key smaller than all the entry, return 0.
	int record_num;
	memcpy(&record_num, (char *)page + PAGE_SIZE - sizeof(int)*2, sizeof(int));

	// corner case
	// root page with no data page
	if(!isDataEntry && record_num == 0) return 0;
	
	// key is NULL (all the way to the leftmost data)
	if(key == NULL) return (isDataEntry ? 1 : 0);

	// insert
	// at index entry, key should be inserted at largest smaller (that position is largest smaller than key) position
	// at data entry, key should be inserted at smallest larger position
	int l = 0, r = record_num;
	while(l < r){
		
		int mid = l + (r-l+1)/2; // mid bias to right
		// extract 'mid'th slot

		int length = 0;
		void* midSlot = ixfileHandle.extractIthSlot(mid, page, length);

		// Attribute attrEmpName;
    	// attrEmpName.length = PAGE_SIZE / 5;  // Each node could only have 4 children
    	// attrEmpName.name = "EmpName";
    	// attrEmpName.type = TypeVarChar;
		// ixfileHandle.printStr(midSlot, attrEmpName);
		// ixfileHandle.printStr(key, attrEmpName);
		// char mid_value[2];
		// char target[2];

		//mid_page, mid_slot, target_page, target_slot;
		// memcpy((char *)mid_value, (char *)midSlot+sizeof(int), sizeof(char)*2);
		//memcpy((char *mid_page, (char *)midSlot + sizeof(int), sizeof(int));
		//memcpy((char *)mid_slot, (char *)midSlot + sizeof(int)*2, sizeof(int));

		// memcpy((char *)target, (char *)key + sizeof(int), sizeof(char)*2);
		//memcpy((char *)target_page, (char *)key + sizeof(int), sizeof(int));
		//memcpy((char *)target_slot, (char *)key + sizeof(int)*2, sizeof(int));
		// cout << "mid: " << mid_value[0] << mid_value[1] << endl; //<< " mid page: " << mid_page << " mid slot: " << mid_slot << endl;
		// cout << "target: " << target[0] << target[1] << endl; //" target page: " << target_page << " target slot: " << target_slot << endl;


		if( compare(key, midSlot, attribute, scanBS) >= 0) l = mid; // if(key > midSlot) l = mid;
		else r = mid-1; // else r = mid-1;

		free(midSlot);
	}
	// if data entry, we should return the same
	// if not data entry, we should return equal or largest smaller
	//if(!isDataEntry && )

	// if(!isDataEntry && l > 0){
 	// 	int length = 0;
	// 	void* midSlot = ixfileHandle.extractIthSlot(l, page, length);
	// 	if(compare(key, midSlot, attribute, scanBS) > 0) l -= 0;
	// 	free(midSlot);
	// }
	cout << "the id: " << l << endl;
	return l;
}

int IndexManager::binarySearch(IXFileHandle &ixfileHandle, const void *key, void* page, const Attribute &attribute, bool isDataEntry, bool scanBS){
	int record_num;
	memcpy(&record_num, (char *)page + PAGE_SIZE - sizeof(int)*2, sizeof(int));

	// corner case
	// root page with no data page
	if(!isDataEntry && record_num == 0) 
		return 0;
	
	// key is NULL (all the way to the leftmost data)

	if(key == NULL)  
	{
		return (isDataEntry ? 1 : 0);
	}
	// insert
	// at index entry, key should be inserted at largest smaller (that position is largest smaller than key) position
	// at data entry, key should be inserted at smallest larger position
	// cout << "record num: " << record_num << endl;
	int l = 1, r = record_num;
	while(l < r){
		//cout << l << ' ' << r << endl;
		int mid = l + (r-l+1)/2; // mid bias to right
		// cout << "mid: " << mid << endl;
		// extract 'mid'th slot

		int length = 0;
		void* midSlot = ixfileHandle.extractIthSlot(mid, page, length);

		// extract key value and compare midSlot with key
		if(compare(key, midSlot, attribute, scanBS) >= 0) l = mid; // if(key >= midSlot) l = mid;
		else r = mid-1; // else r = mid-1;

		free(midSlot);
	}
	
	// corner case
	// index entry and key is smaller than 1st slot
	if(!isDataEntry && l == 1){
		int length = 0;
		void* midSlot = ixfileHandle.extractIthSlot(1, page, length);

		if(compare(key, midSlot, attribute, scanBS) < 0) l = 0; // if(key < 1th slot) l -= 1;
		free(midSlot);
	}

	if(isDataEntry && record_num > 0){
 		int length = 0;
		void* midSlot = ixfileHandle.extractIthSlot(1, page, length);
		if(l == 1 && compare(key, midSlot, attribute, scanBS) < 0) l += 0;
		else ++l;
		free(midSlot);
	}
	return l;
}

void IndexManager::dfs_insert(IXFileHandle &ixfileHandle, const void *key, void* page,   const Attribute &attribute, int& splitPage, int pageNum){
	// find recordSize, free_space, entry type
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
    unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char)); // not sure
	bool isDataEntry = (isDataInt == 1 ? true : false);

	// ixfileHandle.printPageInfo(page, pageNum);

	// bs
	int id = binarySearch(ixfileHandle, key, page, attribute, isDataEntry, false);
	if(!isDataEntry){
		// access the position of slot and shift DATA_KEY_SIZE to point to pointer
		int pPage = findKeyAccessPage(ixfileHandle, attribute, id, page);
		//cout << pPage << ' ' << id << endl;
		void * pg = malloc(PAGE_SIZE);
		ixfileHandle.readPage(pPage, pg);

		// recursion
		dfs_insert(ixfileHandle, key, pg, attribute, splitPage, pPage);
		

		// need to insert slot
		if(splitPage != -1){
			// get indexKey and its length to insert at index entry
			int keyLeng = 0;
			void *indexSlot = formIndexSlot(ixfileHandle, attribute, splitPage, keyLeng);

			//cout << "---------------------" << "PRINT STRING" << "-------------------------" << endl;
			//ixfileHandle.printStr(indexSlot, attribute);
			

			// form insert entry
			
			//void* indexSlot = malloc(KEY_SIZE + PAGE_INFO);
			//formIndexKey(indexSlot, newChildEntry.first, newChildEntry.second);

			// delete origin split entry, index entry push up when spliting
			// if it is an index key, delete the splitted entry
			void* sPage = malloc(PAGE_SIZE);
			ixfileHandle.readPage(splitPage,sPage);
			unsigned char splitisDataInt;
			memcpy(&splitisDataInt, (char *)sPage, sizeof(char));
			if(splitisDataInt != 1)
				simpleDelete(ixfileHandle, sPage, splitPage, 1, isDataEntry);
			free(sPage);
			

			KEY_SIZE = keyLeng;

			int cache = splitPage;
			// reset splitPage
			splitPage = -1;
			// insert new key at 'id+1'th slot
			if(free_space >= sizeof(int)*2 + KEY_SIZE + PAGE_INFO){
				simpleInsert(ixfileHandle, indexSlot, page, splitPage, pageNum, id+1, isDataEntry);
				//impleInsert(ixfileHandle, indexSlot, page, newChildEntry.second, pageNum, id+1, isDataEntry);
			}
			else{
				if(pageNum != 0)split(ixfileHandle, indexSlot, page, splitPage, pageNum, id+1, isDataEntry);
				// If root page is full
				else{
					splitFromRoot(ixfileHandle, indexSlot, page, attribute, splitPage, pageNum, id+1, isDataEntry);
				}
			}
			
			//ixfileHandle.printPageInfo(page, pageNum);
			free(indexSlot);
		}
		free(pg);

		//cout << "######AFTER KEY INSERT######" << endl;
		//ixfileHandle.printPageInfo(page, pageNum);
	}
	else{
		if(free_space >= sizeof(int)*2 + KEY_SIZE){
			simpleInsert(ixfileHandle, key, page,  splitPage, pageNum, id, isDataEntry);
		}
		// need to split
		else split(ixfileHandle, key, page, splitPage, pageNum, id, isDataEntry);

		//cout << "######AFTER DATA INSERT######" << endl;
		//ixfileHandle.printPageInfo(page, pageNum);
	}

}

void IndexManager::simpleInsert(IXFileHandle &ixfileHandle, const void *slot, void* page, int& splitPageId, int pageNum, int id, bool isDataEntry){
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
	// if (id < recordSize)
	shiftDir(ixfileHandle, page, id, recordSize, id+1);
	ixfileHandle.insertIthSlot(id, slot, page, isDataEntry, KEY_SIZE);
	// write the page to file.
	ixfileHandle.writePage(pageNum, page);
}

void IndexManager::split(IXFileHandle &ixfileHandle, const void *key, void* page, int& splitPageId, int pageNum, int id, bool isDataEntry){
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
	void *splitPage = malloc(PAGE_SIZE);
	int splitPageNum;

	// find deleting page at header
	void *headPage = malloc(PAGE_SIZE);
	ixfileHandle.readHeader(headPage);
	int headRecordSize, headFreeSpace;
	ixfileHandle.retrieveRecordsizeAndFreespace(headPage, headRecordSize, headFreeSpace);


	if(headRecordSize){
		// retrieve the last slot deleting page
		int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(headRecordSize);

		memcpy(&splitPageNum, (char *)headPage + slotPosition, sizeof(int));

		// update recordsize, freespace, flag of header
		headRecordSize -= 1;
		headFreeSpace += 3*sizeof(int); // 4byte length, 4 byte offset, 4 byte data
		memcpy((char *)headPage + PAGE_SIZE - sizeof(int)*2, &headRecordSize, sizeof(int));
		memcpy((char *)headPage + PAGE_SIZE - sizeof(int), &headFreeSpace, sizeof(int));
		ixfileHandle.writeHeader(headPage); // write back to header

		ixfileHandle.readPage(splitPageNum, splitPage);
		// reset flag
		//flag: index:0/data:1
		unsigned char isDataInt = (isDataEntry ? 1 : 0);
		memcpy((char*)splitPage, &isDataInt, sizeof(unsigned char));

	}
	else{
		// if can't find, create new page
		ixfileHandle.createEmptyPage(splitPage, isDataEntry ? true : false, -1);
		ixfileHandle.appendPage(splitPage);
		splitPageNum = ixfileHandle.getNumberOfPages()-1;
	}
	free(headPage);

	//cout << "########################################################## SPLIT from page " << pageNum << " toward page " << splitPageNum << endl;

	// compact
	int splitPoint = recordSize/2;
	
	// L2
	//ixfileHandle.printPageInfo(splitPage, splitPageNum);
	compact(ixfileHandle, page, splitPoint+1, recordSize, splitPage, DATA_START+sizeof(int), 1);
	// L1

	compact(ixfileHandle, page, 1, splitPoint, page, DATA_START + (isDataEntry ? 0 : PAGE_INFO), 1);


//	// debug
//	if(!isDataEntry){
//		int p0;
//		memcpy(&p0, (char *)page+DATA_START, sizeof(int));
//		cout << "[SPLIT] first page in 0st root child: " << p0 << endl;
//
//	}


	

	// void* newPage = malloc(PAGE_SIZE);
	// //compact(ixfileHandle, page, 1, splitPoint, newPage, DATA_START+sizeof(int), 1);
	
	// memcpy((char *)page, (char *)newPage, PAGE_SIZE);
	// free(newPage);
	

	
	
	
	// linkPage
	ixfileHandle.linkPage(page, splitPage, pageNum, splitPageNum);

	//ixfileHandle.printPageInfo(page, pageNum);
	//ixfileHandle.printPageInfo(splitPage, splitPageNum);
	// insert data

	if(id > splitPoint){
		shiftDir(ixfileHandle, splitPage, id-splitPoint, recordSize-splitPoint, id-splitPoint+1);
		ixfileHandle.insertIthSlot(id-splitPoint, key, splitPage, isDataEntry, KEY_SIZE);
	}
	else{
		shiftDir(ixfileHandle, page, id, splitPoint, id+1);
		ixfileHandle.insertIthSlot(id, key, page, isDataEntry, KEY_SIZE);
	}
	//if(!isDataEntry) ixfileHandle.printPageInfo(splitPage, splitPageNum);

	
	// copy the split pointer as the split first's pointer
	if(!isDataEntry){
		// paste the split entry's pointer to first pointer
		int l=0;
		void* splitEntry = ixfileHandle.extractIthSlot(1, splitPage, l);
		
		memcpy((char * )splitPage+DATA_START, (char *)splitEntry + l - sizeof(int), sizeof(int));
		//simpleDelete(ixfileHandle, splitPage, splitPageNum, 1, isDataEntry);
		free(splitEntry);
		
	}
	
	
	// setting splitPageId
	splitPageId = splitPageNum;
	
    // write page
	ixfileHandle.writePage(pageNum, page);
	ixfileHandle.writePage(splitPageNum, splitPage);

	// free
	free(splitPage);
}

RC IndexManager::dfs_delete(IXFileHandle &ixfileHandle, void *key, void* page, const Attribute &attribute, pair<void*, int>& childEntry, int pageNum
, void* parentPage, int pageIdonParent)
{


	// find recordSize, free_space, entry type	
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
    unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char)); // not sure
	bool isDataEntry = (isDataInt == 1 ? true : false);

	// bs
	int id = naiveBinarySearch(ixfileHandle, key, page, attribute, isDataEntry, false);
	
	// when the page is data entry, we need to check whether the key is in the page.
	// note that we don't need to check for index page.
	if (isDataEntry){

		int length = 0;
		
		void* targetSlot = ixfileHandle.extractIthSlot(id, page, length);
		int cmp = compare(key, targetSlot, attribute, false);
		if(cmp != 0) {
			//cout << "child " << endl;
			//preOrderTraversal(ixfileHandle, attribute, )
			// ixfileHandle.printPageInfo(page, pageNum);
			
			preOrderTraversal(ixfileHandle, attribute, pageNum, 1);
			return -1;
		}
	}
	
	if (!isDataEntry){
		// access the position of slot and shift DATA_KEY_SIZE to point to pointer
		int pPage = findKeyAccessPage(ixfileHandle, attribute, id, page);
		void * pg = malloc(PAGE_SIZE);
		ixfileHandle.readPage(pPage, pg);
		
		void* oldChildEntryKey = malloc(PAGE_SIZE);
		pair<void* , int> oldChildEntry = {oldChildEntryKey, -1};
		// recursion
		RC rc;
		rc = dfs_delete(ixfileHandle, key, pg, attribute, oldChildEntry, pPage, page, id);
		
		if (rc != 0) return -1;

		if (oldChildEntry.second == -2){
			// means this is under redistribution
			// 1. find the key id (in this case, the key would not be the same with oldChildEntry.first)
			// 2. paste the key into it
			int deleteId = naiveBinarySearch(ixfileHandle, oldChildEntry.first, page, attribute, isDataEntry, false);
			void* originEntry;
			int pl;
			originEntry = ixfileHandle.extractIthSlot(deleteId, page, pl);
			int newPointer;
			memcpy(&newPointer, (char *)originEntry+pl - sizeof(int), sizeof(int));
			void* newEntry = malloc(pl);
			formIndexKey(newEntry, originEntry, newPointer, pl);
			simpleInsert(ixfileHandle, newEntry, page, pageNum, pageNum, deleteId, isDataEntry);

		}

		else if (oldChildEntry.second != -1){
			// if the oldChildEntry is not null, we should delete this childEntry.
		
			//ixfileHandle.printStr(oldChildEntry.first,attribute);
			int deleteId = naiveBinarySearch(ixfileHandle, oldChildEntry.first, page, attribute, isDataEntry, false);
			
			void* targetKey = NULL;
			int childLength;
			targetKey = ixfileHandle.extractIthSlot(deleteId, oldChildEntry.first,childLength);
			
			// int length = 0;
			// void* targetSlot = ixfileHandle.extractIthSlot(deleteId, page, length);
			// ixfileHandle.printStr(oldChildEntry.first, attribute);
			// cout << "childNum" << oldChildEntry.second <<endl;
			//if(compare(oldChildEntry.first, targetSlot, attribute, false) != 0) return -1;
			//cout << "free space after delete" << free_space + (sizeof(int)*2 + childLength) << endl;
			if (parentPage == NULL){
				// if this is root page, we don't need to merge with other
				// delete this key 
				//int deleteId = binarySearch(ixfileHandle, oldChildEntry.first, page, attribute, isDataEntry);
				
				simpleDelete(ixfileHandle,  page, pageNum, deleteId, isDataEntry);
				// //cout << "root page simple delete" << endl;
				// ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
				// // if the root still has entry
				// if(recordSize != 0) 
				// {	
				// 	cout << "don't need to merge root " << endl;
				// 	return 0;
				// }
				// cout << "the old child second" << oldChildEntry.second << endl;
				// void* childPage = malloc(PAGE_SIZE);
				// ixfileHandle.readPage(oldChildEntry.second, childPage);
				// unsigned char cisDataInt;
				// memcpy(&cisDataInt, (char *)childPage, sizeof(unsigned char)); 
				// bool cisDataEntry = (cisDataInt == 1 ? true : false);
				// free(childPage);

				// if(cisDataEntry){
				// 	cout << "keep the root" << endl;
				// 	// if root is the only index page, keep root
				// 	memcpy((char *)page + DATA_START, &oldChildEntry.second, sizeof(int));
				// 	ixfileHandle.writePage(pageNum, page);
				// }
				// else{
				// 	// if root is not the only index page, delete this root and copy new root to page 0
				// 	int newRootNum = oldChildEntry.second;
				// 	// copy the new root to page 0 and delete new page
				// 	void* new_root = malloc(PAGE_SIZE);
				// 	ixfileHandle.readPage(newRootNum, new_root);
				// 	ixfileHandle.writePage(0, new_root);
				// 	ixfileHandle.deletePage(newRootNum);
				// 	free(new_root);
				// }	
				// // cout << "after swaping page, the root is :" << endl;
				// preOrderTraversal(ixfileHandle, attribute, 0, 1);
				
			}
			// else if(free_space + (sizeof(int)*2 + KEY_SIZE) < PAGE_SIZE*MIN_UTIL_RATIO){
			else{
				
				simpleDelete(ixfileHandle, page, pageNum, deleteId, isDataEntry);
			}

			//}
			// else{
			
			// 	simpleDelete(ixfileHandle, page, pageNum, deleteId, isDataEntry);
			// 	cout << "after simple delete, then merge" << endl;
			// 	merge(ixfileHandle, oldChildEntry.first, page, childEntry, pageNum, isDataEntry, parentPage, pageIdonParent );
			// }

			
		}
		else{
			ixfileHandle.writePage(pageNum, page);
		}

		
	}
	else{
		// if(free_space + (sizeof(int)*2 + KEY_SIZE ) < PAGE_SIZE*MIN_UTIL_RATIO){
			// don't need to merge
		
		simpleDelete(ixfileHandle,  page, pageNum, id, isDataEntry);
		// }else{
		// 	simpleDelete(ixfileHandle, page,  pageNum, id, isDataEntry);
		// 	cout << " need to merge" << endl;
		// 	merge(ixfileHandle, key, page, childEntry, pageNum, isDataEntry, parentPage, pageIdonParent );
		// }

	}
	return 0;

}
void IndexManager::simpleDelete(IXFileHandle &ixfileHandle, void* page, int pageNum, int id, bool isDataEntry)
{
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);

	// shift the slot dir after deleted slot dir to replace the original slot dir,
	// then the deleted slot dir is gone.
	//cout << "before shift" << endl;
	shiftDir(ixfileHandle, page, id+1, recordSize, id);
	// make all the slot after deleted slot being compact
	void* newPage = malloc(PAGE_SIZE);
	
	// int length = 0;
	// void* targetSlot = ixfileHandle.extractIthSlot(id, page, length);
	// int target;
	// memcpy(&target, (char *)targetSlot, sizeof(int));
	// cout << target << endl;
	// cout << "recordSize: " << recordSize << endl;
	// cout << "before compact" << endl;
	if (!isDataEntry)
		compact(ixfileHandle, page, 1, recordSize-1, newPage, DATA_START + sizeof(int), 1);
	else
		compact(ixfileHandle, page, 1, recordSize-1, newPage, DATA_START, 1);

	//cout << "after compact" << endl;
	memcpy((char *)page, (char *)newPage, PAGE_SIZE);


	free(newPage);
	// write the page back
	ixfileHandle.writePage(pageNum, page);

}

void IndexManager::merge(IXFileHandle &ixfileHandle, const void *key, void* page, pair<void*, int>& childEntry, int pageNum, bool isDataEntry,
void* parentPage, int pageIdonParent)
{
	// get page record size and free space
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);

	// get parent page record size and free space
	int precordSize, pfree_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(parentPage, precordSize, pfree_space);


	//void* splittingKey;
	if (pageIdonParent == 0 && precordSize == 0){
		
		// if this data page is the only page, so you cannot find any sibling
		return;
		
	}

	else if (pageIdonParent != precordSize){
		// find the right sibling
		// first, get the right page num from parent page
		void* rightKey = NULL;
		int rightKeyLen = 0;
		
		rightKey = ixfileHandle.extractIthSlot( pageIdonParent+1, parentPage, rightKeyLen);
		
		int rightPageNum;
		memcpy(&rightPageNum, (char*)rightKey + rightKeyLen - sizeof(int), sizeof(int));
		//cout << "right: " << rightPageNum << endl;

		// read the right sibling
		void* rightSibling = malloc(PAGE_SIZE);
		ixfileHandle.readPage(rightPageNum, rightSibling);

		// get the free space in rightSibling

		int rightRecordSize, rightFree_space;
		ixfileHandle.retrieveRecordsizeAndFreespace(rightSibling, rightRecordSize, rightFree_space);

		// first we can check whether we can redistribute
		// int success = redistribute(ixfileHandle, key, childEntry ,page, pageNum, rightSibling, rightPageNum, parentPage, pageIdonParent, true);

		// // // if the key could redistribute, return
		// if (success == 0) return;

		// //check if there is enough space to merge
		int add_size = (isDataEntry) ? 0 : rightKeyLen + sizeof(int)*2;
		if(free_space < (PAGE_SIZE - sizeof(int)*2 - 9 - rightFree_space + add_size) )
		{
			// nothing we can do
			cout << "nothing we can do" << endl;
			return;
		}	


		
		// 
		if (!isDataEntry){
			
			// pull down the right key(equals to insert right key)
			// get the key first, we don't need its pointer
			void* key = NULL;
			int l;
			int pointer;
			
			key = ixfileHandle.extractIthSlot(pageIdonParent+1, parentPage, l);
		
			// get the first pointer of rightSubling to form new index key
			int firstPageNum;
			memcpy(&firstPageNum, (char *)rightSibling+DATA_START, sizeof(int));
			void* pulledIndexKey = malloc(KEY_SIZE+PAGE_INFO);
			formIndexKey(pulledIndexKey, key, firstPageNum, l);
			// ixfileHandle.printStr(pulledIndexKey, attrEmpName);
			
			// insert the new formed key to page
			int recordSize, free_space;
			ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
			ixfileHandle.insertIthSlot(recordSize+1, pulledIndexKey, page, isDataEntry, KEY_SIZE);
			
			// copy the all the key in the rightSibling to this page
			void* tkey = NULL;
			int keyLen = 0;
			for(int r=1; r <= rightRecordSize; r++) {
				tkey = NULL;
				keyLen = 0;
				tkey = ixfileHandle.extractIthSlot( r , rightSibling, keyLen);
				// since in insertIthSlot, the keyLen would add up PAGE_INFO if this is index entry
				// so we substract the PAGE_INFO first.
				ixfileHandle.insertIthSlot(recordSize+1+r, tkey, page, isDataEntry, keyLen - PAGE_INFO);
			}
			//cout << "after copy" << endl;
			
			// 
			//tkey = ixfileHandle.extractIthSlot( 1 , page, keyLen);
			
			// modified childEntry, since we merge with right sibling, the child entry would be the 
			// first entry in the rightSibling, and pageNum would be this page.
			settingChildEntry(ixfileHandle, rightSibling, childEntry, pageNum, isDataEntry);
			

		}
		else{
			int recordSize, free_space;
			ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
			// move all data on rightSibling to this page
			int lastSlotPosition = PAGE_SIZE - (free_space - sizeof(int)*2 - sizeof(int)*2*recordSize);
			void* tkey = NULL;
			int keyLen = 0;
			for(int r=1; r <= rightRecordSize; r++) {
				tkey = NULL;
				keyLen = 0;
				tkey = ixfileHandle.extractIthSlot( r , rightSibling, keyLen);
				ixfileHandle.insertIthSlot(recordSize+r, tkey, page, isDataEntry, keyLen);
			}
	
			// take out the first entry in rightSibling as childEntry
			settingChildEntry(ixfileHandle, rightSibling, childEntry, pageNum, isDataEntry);
			//cout << "here " << endl;

		}
		// ixfileHandle.printPageInfo(parentPage, 0);
		cout << "merge l page " << pageNum << " with  r page" << rightPageNum << endl;

		// cache the rightPage's next page and link with this page
		void* nxtPage = malloc(PAGE_SIZE);
		int nxtPageNum;
		memcpy(&nxtPageNum, (char *)rightSibling + FLAG_SIZE + LINK_SIZE, sizeof(int));
		ixfileHandle.readPage(nxtPageNum, nxtPage);
		ixfileHandle.linkPage(page, nxtPage, pageNum, nxtPageNum);
		ixfileHandle.writePage(nxtPageNum, nxtPage);
		ixfileHandle.writePage(pageNum, page);
		ixfileHandle.deletePage((unsigned int)rightPageNum);
		free(nxtPage);
		
	}
	else if(pageIdonParent != 0){
		
		// find left sibling
		// first, get the left page num from parent page
		//cout << "in left sibling." << endl;
		//cout << "page id on parent " <<  pageIdonParent << endl;
		//cout << "parent recordSize : " << precordSize << endl;
		void* leftKey = NULL;
		int leftKeyLen = 0;
		int leftPageNum;
		if(pageIdonParent == 1){
			leftKey = ixfileHandle.extractIthSlot(1, parentPage, leftKeyLen);

			memcpy(&leftPageNum, (char*)parentPage+DATA_START, sizeof(int));
		}
		else{
			leftKey = ixfileHandle.extractIthSlot( pageIdonParent-1, parentPage, leftKeyLen);
			memcpy(&leftPageNum, (char*)leftKey + leftKeyLen - sizeof(int), PAGE_INFO);
		}	
		
		

		// read the left sibling
		void* leftSibling = malloc(PAGE_SIZE);
		ixfileHandle.readPage(leftPageNum, leftSibling);

		// get the free space in lefttSibling

		int leftRecordSize, leftFree_space;
		ixfileHandle.retrieveRecordsizeAndFreespace(leftSibling, leftRecordSize, leftFree_space);

		//merge(ixfileHandle, leftKey, leftSibling, childEntry, leftPageNum, isDataEntry, parentPage, pageIdonParent-1);
	//}
		// int success = redistribute(ixfileHandle, key, childEntry, page, pageNum, leftSibling, leftPageNum, parentPage, pageIdonParent, false);

		// // if the key could redistribute, return
		// if (success == 0) return;
		
		int add_size = (isDataEntry) ? 0 : leftKeyLen + sizeof(int)*2; // if it is index, we should pull down
		if(leftFree_space < (PAGE_SIZE - sizeof(int)*2 - 9 - free_space + add_size) )
			{
			// nothing we can do
				return;
			}
		

		// 
		if (!isDataEntry){
			
			// pull down the left key(equals to insert left key)
			// get the key first, we don't need its pointer
			// ixfileHandle.printPageInfo(leftSibling, leftPageNum);
			void* key = NULL;
			int l, pointer;

			key = ixfileHandle.extractIthSlot(pageIdonParent, parentPage, l);

			// // get the first pointer of this page to form new index key
			int firstPageNum;
			memcpy(&firstPageNum, (char *)page+DATA_START, sizeof(int));
			void* pulledIndexKey = malloc(KEY_SIZE+PAGE_INFO);
			formIndexKey(pulledIndexKey, key, firstPageNum, l);
			// insert the new formed key to left sibling
			int leftRecordSize, leftFree_space;
			ixfileHandle.retrieveRecordsizeAndFreespace(leftSibling, leftRecordSize, leftFree_space);
			ixfileHandle.insertIthSlot(leftRecordSize+1, pulledIndexKey, leftSibling, isDataEntry, KEY_SIZE);

			// move all data on this page to sibling
			void* tkey = NULL;
			int keyLen = 0;
			for(int r=1; r <= recordSize; r++) {
				tkey = NULL;
				keyLen = 0;
				tkey = ixfileHandle.extractIthSlot( r , page, keyLen);
				int blob;
				//simpleInsert(ixfileHandle, tkey, leftSibling, blob, leftPageNum, leftRecordSize + 1 + r, isDataEntry);
				ixfileHandle.insertIthSlot(leftRecordSize+1+r, tkey, leftSibling, isDataEntry, keyLen - PAGE_INFO);

			}
			settingChildEntry(ixfileHandle, page, childEntry, leftPageNum, isDataEntry);

			// move the first pointer of leftSilbing to this page
			memcpy((char *)leftSibling+DATA_START, (char *)page+DATA_START, sizeof(int));
			// modified childEntry

		}
		else{

			int leftRecordSize, leftFree_space;
			ixfileHandle.retrieveRecordsizeAndFreespace(leftSibling, leftRecordSize, leftFree_space);
			// move all data on this page to leftSibling
			// ixfileHandle.printPageInfo(leftSibling, leftPageNum);
			void* tkey = NULL;
			int keyLen = 0;
			//cout << "left record size:" << leftRecordSize << endl;
			for(int r=1; r <= recordSize; r++) {
				tkey = NULL;
				keyLen = 0;
				tkey = ixfileHandle.extractIthSlot( r , page, keyLen);
				int blob;
				//simpleInsert(ixfileHandle, tkey, page, blob, pageNum, r, isDataEntry);
				ixfileHandle.insertIthSlot(leftRecordSize+r, tkey, leftSibling, isDataEntry, keyLen);

				// tkey = ixfileHandle.extractIthSlot( r , page, keyLen);
				// Attribute attrEmpName;
				// attrEmpName.length = PAGE_SIZE / 5;  // Each node could only have 4 children
				// attrEmpName.name = "EmpName";
				// attrEmpName.type = TypeVarChar;
				// ixfileHandle.printStr(tkey, attrEmpName);
				//ixfileHandle.insertIthSlot(r, tkey, page, isDataEntry, keyLen-sizeof(int));
			}
			settingChildEntry(ixfileHandle, page, childEntry, leftPageNum, isDataEntry);

			// modified childEntry


		}
		cout << "merge l page " << leftPageNum << " with  r page" << pageNum << endl;

		//cache the page's next page and link with leftSibling
		// void* prevPage = malloc(PAGE_SIZE);
		// int prevPageNum;
		// memcpy(&prevPageNum, (char *)leftSibling + FLAG_SIZE , sizeof(int));
		// ixfileHandle.readPage(prevPageNum, prevPage);
		// ixfileHandle.linkPage(prevPage, leftSibling, prevPageNum, leftPageNum;
		int nxtPageNum = -1;
		memcpy((char *)leftSibling + FLAG_SIZE +LINK_SIZE, &nxtPageNum, sizeof(int) );
		ixfileHandle.writePage(leftPageNum, leftSibling);
		ixfileHandle.deletePage((unsigned int)pageNum);
		//Attribute attrAge;
		//attrAge.length = 4;
		//attrAge.name = "age";
		//attrAge.type = TypeInt;
		//cout << "test print" << endl;
		//preOrderTraversal(ixfileHandle, attrAge, leftPageNum, 1);

	}

}

int IndexManager::redistribute(IXFileHandle &ixfileHandle, const void *key, pair<void*, int>& childEntry, void* page, int pageNum, void* sibling, int siblingNum, void* parent, int pageIdonParent, bool borrowFromRight)
{
	int sRecordSize, sFree_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(sibling, sRecordSize, sFree_space);
	
	int recordSize, free_space;
	ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);

	unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char)); // not sure
	bool isDataEntry = (isDataInt == 1 ? true : false);

	unsigned char sisDataInt;
	memcpy(&sisDataInt, (char *)page, sizeof(unsigned char)); // not sure
	bool sisDataEntry = (sisDataInt == 1 ? true : false);

	// fetch the first key at sibling
	void* borrowKey;
	int l;
	if (borrowFromRight)
		borrowKey = ixfileHandle.extractIthSlot(1, sibling, l);
	else
		borrowKey = ixfileHandle.extractIthSlot(sRecordSize, sibling, l);

	if(sFree_space + (l + sizeof(int)*2) >= PAGE_SIZE*MIN_UTIL_RATIO ){
		return -1;
	}

	else{
		//cout << "redistribute sibling page " << siblingNum << " to " << pageNum << endl;
		int blob;

		if (borrowFromRight){
			//ixfileHandle.insertIthSlot(recordSize+1, borrowKey, page, isDataEntry, l);
			simpleInsert(ixfileHandle, borrowKey,  page, blob, pageNum, recordSize+1 ,isDataEntry);
			simpleDelete(ixfileHandle, sibling, siblingNum, 1, isDataEntry);
			ixfileHandle.readPage(siblingNum, sibling); // update sibling after redistribute
			int childPageNum = -2;
			settingChildEntry(ixfileHandle, sibling, childEntry, childPageNum, isDataEntry);
			// void* newIndexKey;
			// int ll;
			// newIndexKey = ixfileHandle.extractIthSlot(1, sibling, ll);
			// void* newIndex;
			// if(!isDataEntry){
				
			// 	newIndex = malloc(ll);
			// }
			// else{
			// 	ll = ll+PAGE_INFO;
			// 	newIndex = malloc(ll);
			// }
			// // void* parentKey;
			// // int pl;
			// // parentKey = ixfileHandle.extractIthSlot(pageIdonParent, parent, pl);
			// // int pointer;
			// // memcpy(&pointer, (char *)parentKey+ll-sizeof(int), sizeof(int));

			// formIndexKey(newIndex, newIndexKey, siblingNum, ll);
			//setting
			//ixfileHandle.insertIthSlot(pageIdonParent, newIndex, parent, false, ll);
		}
		else{
			
			simpleInsert(ixfileHandle, borrowKey,  page, blob, pageNum, 1 ,isDataEntry);
			simpleDelete(ixfileHandle, sibling, siblingNum, recordSize, isDataEntry);
			ixfileHandle.readPage(pageNum, page); // update page after redistribution
			int childPageNum = -2;
			settingChildEntry(ixfileHandle, page, childEntry, childPageNum, isDataEntry);

			// void* newIndexKey;
			// int ll;
			// newIndexKey = ixfileHandle.extractIthSlot(1, page, ll);
			// void* newIndex;
			// if(!isDataEntry){
			// 	newIndex = malloc(ll);
			// }
			// else{
			// 	ll = ll+PAGE_INFO;
			// 	newIndex = malloc(ll);
			// }
			// // void* parentKey;
			// // int pl;
			// // parentKey = ixfileHandle.extractIthSlot(pageIdonParent, parent, pl);
			// // int pointer;
			// // memcpy(&pointer, (char *)parentKey+ll-sizeof(int), sizeof(int));

			// formIndexKey(newIndex, newIndexKey, pageNum, ll);
			//ixfileHandle.insertIthSlot(pageIdonParent, newIndex, parent, false, ll);

		}
		return 0;
	}

}


void IndexManager::splitFromRoot(IXFileHandle &ixfileHandle, const void *key, void* rootPage, const Attribute &attribute, int& splitPageId, int rootPageNum, int id, bool isDataEntry){
	// create new page pX
	void *newKeyPage = malloc(PAGE_SIZE);
	ixfileHandle.createEmptyPage(newKeyPage, false, -1);
	ixfileHandle.appendPage(newKeyPage);
	int newKeyPageNum = ixfileHandle.getNumberOfPages()-1;

	// debug
	// ixfileHandle.printPageInfo(rootPage, rootPageNum);
	int p0;
	memcpy(&p0, (char *)rootPage+DATA_START, sizeof(int));
	//cout << "first page in root: " << p0 << endl;

	// copy all the data from root to pX
	memcpy((char* )newKeyPage, (char *)rootPage, PAGE_SIZE);

	// set 0th key of root to be pX, and update freeSpace & recordSize
	memcpy((char* )rootPage + DATA_START, &newKeyPageNum, sizeof(int));
	int free_space = PAGE_SIZE - DATA_START - sizeof(int)*2 - sizeof(int);
	int recordSize = 0;
	memcpy((char* )rootPage + PAGE_SIZE - sizeof(int), &free_space, sizeof(int));
	memcpy((char* )rootPage + PAGE_SIZE - sizeof(int)*2, &recordSize, sizeof(int));
	
	// split from pX
	split(ixfileHandle, key, newKeyPage, splitPageId, newKeyPageNum, id, isDataEntry);
	//cout << splitPageId << endl;

//	// debug
//	memcpy(&p0, (char *)newKeyPage+DATA_START, sizeof(int));
//	cout << "first page in 0st root child: " << p0 << endl;

	// set 1th key of root to be splitPageId, and update freeSpace & recordSize
	int keyLeng = 0;
	void *indexSlot = formIndexSlot(ixfileHandle, attribute, splitPageId, keyLeng);
	//cout << "---------------------" << "PRINT STRING" << "-------------------------" << endl;
	//ixfileHandle.printStr(indexSlot, attribute);

	// // need to delete the first key at split page
	void* sPage = malloc(PAGE_SIZE);
	ixfileHandle.readPage(splitPageId,sPage);
	unsigned char splitisDataInt;
	memcpy(&splitisDataInt, (char *)sPage, sizeof(char));
	//if(splitisDataInt != 1)
	simpleDelete(ixfileHandle, sPage, splitPageId, 1, isDataEntry);
	free(sPage);


	KEY_SIZE = keyLeng;

	// reset splitPage
	splitPageId = -1;
	simpleInsert(ixfileHandle, indexSlot, rootPage, splitPageId, rootPageNum, 1, isDataEntry);
	// ixfileHandle.printPageInfo(rootPage, rootPageNum);

	free(indexSlot);
	free(newKeyPage);
}


// make slot between lowSlot & upSlot at pageA COMPACT at pageB (pageA & pageB could be same page)
void IndexManager::compact(IXFileHandle &ixfileHandle, void* page, int lowSlot, int upSlot, void* newPage, int startOffset, int startSlot){
	
	void* cache = malloc(PAGE_SIZE);
	memcpy((char *)cache, (char *)newPage, PAGE_SIZE);
	free(cache);
	
	// copy all the data from page to newPage
	memcpy((char *)newPage, (char *)page, PAGE_SIZE);

	// collect all the offset, length and new slot_id, then sort based on offset
	vector<pair<int, vector<int>>> p;
	for(int i = lowSlot; i <= upSlot; ++i){
		int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
		int offset, length;
		memcpy(&offset, (char *)newPage + slotPosition, sizeof(int));
		memcpy(&length, (char *)newPage + slotPosition + sizeof(int), sizeof(int));
		vector<int> tmp = {i-lowSlot+startSlot, length};
		p.push_back({offset, tmp});
	}
	sort(p.begin(), p.end());




	// shift directory
	if(lowSlot-startSlot != 0) shiftDir(ixfileHandle, newPage, lowSlot, upSlot, startSlot);

	// assign data to newpage one by one
	int curStart = startOffset;
	for(auto& pi : p){
		int offset = pi.first;
		int slotId = pi.second[0];
		int l = pi.second[1];

		// shift data to make it compact
		void * slot = malloc(l);
		memcpy((char *)slot, (char *)page + offset, l);
		memcpy((char *)newPage + curStart, slot, l);
		//cout << "Offset: " << offset << " slotId: " << slotId <<  " length: " << l << endl;
		//cout << "Make data from: " << offset <<  " to: " << curStart << endl;
		free(slot);

		// change offset record
		int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(slotId);
		//cout << "ID:" << slotId << " New slot offset at: " << curStart <<  " At: " << slotPosition << endl << endl;
		memcpy((char *)newPage + slotPosition, &curStart, sizeof(int));
		memcpy((char *)newPage + slotPosition + sizeof(int), &l, sizeof(int));

		curStart += l;
	}

	// update freespace, slotNum
	int recordSize = upSlot - lowSlot + startSlot; // not sure
	int free_space = PAGE_SIZE - curStart - recordSize*sizeof(int)*2 - sizeof(int)*2;
	memcpy((char *)newPage + PAGE_SIZE - 2*sizeof(int), &recordSize, sizeof(int));
	memcpy((char *)newPage + PAGE_SIZE - sizeof(int), &free_space, sizeof(int));

	

}
void IndexManager::shiftDir(IXFileHandle &ixfileHandle, void* page, int lowSlot, int upSlot, int startSlot){

	if(lowSlot > startSlot){
		for(int i = startSlot; i <= startSlot + (upSlot-lowSlot); ++i){
			void* slotDir = ixfileHandle.extractIthSlotDir(i-startSlot+lowSlot, page);
			int shiftPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
			memcpy((char *)page + shiftPosition, slotDir, sizeof(int)*2);
			free(slotDir);
			//cout << "move " << i-startSlot+lowSlot << "th slot directory" " toward " << i << " at position: " << shiftPosition << endl;
		}
	}
	else{
		for(int i = startSlot + (upSlot-lowSlot); i >= startSlot; --i){
			void* slotDir = ixfileHandle.extractIthSlotDir(i-startSlot+lowSlot, page);
			int shiftPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
			memcpy((char *)page + shiftPosition, slotDir, sizeof(int)*2);
			free(slotDir);
			//cout << "move " << i-startSlot+lowSlot << "th slot directory" " toward " << i << " at position: " << shiftPosition << endl;
		}
	}
}

void IndexManager::settingChildEntry(IXFileHandle &ixfileHandle, void* splitPage, pair<void*, int>& childEntry, int& splitPageNum, bool isDataEntry){
	void* indexSlot;
	int l;
	indexSlot = ixfileHandle.extractIthSlot(1, splitPage, l);
	l = (isDataEntry == true) ? l : l - PAGE_INFO;
	memcpy(childEntry.first, (char*)indexSlot, l);
	childEntry.second = splitPageNum;
	free(indexSlot);
}

void* IndexManager::formIndexSlot(IXFileHandle &ixfileHandle, const Attribute &attribute, int& splitPage, int& keyLeng){
	void* page = malloc(PAGE_SIZE);
	ixfileHandle.readPage(splitPage, page);

	// extract 1st slot
	int key_l = 0;
	void* cpSlot = ixfileHandle.extractIthSlot(1, page, key_l);

	// check the type of split page
	unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char));
	bool isDataEntry = (isDataInt == 1 ? true : false);

	//
	int slotLeng = isDataEntry ? key_l + sizeof(int) : key_l;
	keyLeng = slotLeng - sizeof(int);
	void* indexSlot = malloc(slotLeng);
	memcpy((char *)indexSlot, (char *)cpSlot, keyLeng);
	memcpy((char *)indexSlot + keyLeng, &splitPage, sizeof(int));
	//cout << "FORM INDEX SLOT: " << slotLeng << endl;

	free(cpSlot);
	free(page);
	return indexSlot;
}
void IndexManager::formIndexKey(void* slot, void* key, int pageNum, int length ){
	// this function replace the pointer in key with pageNum
	// input : length is the length of origin key
	memcpy((char *)slot, (char *)key, length - sizeof(int));
	memcpy((char *)slot+length-sizeof(int), &pageNum, sizeof(int));
}

void IndexManager::initSlotSize(int& slot_size){
	SLOT_SIZE = slot_size;
	KEY_SIZE = slot_size + KEY_TAIL;
}

int IndexManager::findKeyAccessPage(IXFileHandle& ixfileHandle, const Attribute &attribute, int& id, void* page){
	int pPage;
	// the first pointer, init the first data
	if(id == 0){
		memcpy(&pPage, (char *)page + DATA_START, sizeof(int));
		// if no data_index, create 1 data page
		// assert that such a case won't happen when scan
		if(pPage == 0){
			ixfileHandle.createFirstData(page, pPage);

			// update free space
			int free_space;
			memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
			free_space -= sizeof(int);
			memcpy((char *)page + PAGE_SIZE - sizeof(int), &free_space, sizeof(int));
		}
	}
	else{
		int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(id);
		int offset, length;
		memcpy(&offset, (char *)page + slotPosition, sizeof(int));
		memcpy(&length, (char *)page + slotPosition + sizeof(int), sizeof(int));
		memcpy(&pPage, (char *)page + offset + length - sizeof(int), sizeof(int)); // fixed
	}
	return pPage;
}

int IndexManager::compare(const void* keyA, void* keyB, const Attribute &attribute, bool scanBS) const{
	// return 0: same, 1: keyA > keyB, -1: keyA < keyB
	int ridShift = sizeof(int);
	int res;

	if(attribute.type == TypeVarChar){
		res = compareV(keyA, keyB);
		if(res != 0) return res;

		int l;
		memcpy(&l, (char *)keyA, sizeof(int));
		ridShift += l;
	}
	else if(attribute.type == TypeInt){
		int vA, vB;
		memcpy(&vA, (char *)keyA, sizeof(int));
		memcpy(&vB, (char *)keyB, sizeof(int));
		res = compareV(vA, vB);
		if(res != 0) return res;
	}
	else{
		float vA, vB;
		memcpy(&vA, (char *)keyA, sizeof(float));
		memcpy(&vB, (char *)keyB, sizeof(float));
		res = compareV(vA, vB);
		if(res != 0) return res;
	}
	// don't have to compare rid if scanBS is true
	if(scanBS) return res;

	// if the value is same, compare rid, assert that 2 rids are absolutely different
	return compareRID(keyA, keyB, ridShift);
}

int IndexManager::compareV(int& a, int& b) const{
	return (a > b ? 1 : (a < b ? -1 : 0));
}

int IndexManager::compareV(float& a, float& b) const{
	return (a > b ? 1 : (a < b ? -1 : 0));
}

int IndexManager::compareV(const void* a, const void* b) const{
	int res = -2; // init state
	int lA, lB;
	memcpy(&lA, (char *)a, sizeof(int));
	memcpy(&lB, (char *)b, sizeof(int));
	char* vA = new char[lA], *vB = new char[lB];
	memcpy((char *)vA, (char *)a+ sizeof(int), lA);
	memcpy((char *)vB, (char *)b+ sizeof(int), lB);

	int i = 0;
	while(i < lA && i < lB){
		//cout << vA[i] << ' ' << vB[i] << endl;
		if(vA[i] != vB[i]){
			res =  vA[i] > vB[i] ? 1 : -1;
			break;
		}
		++i;
	}
	if(res == -2){
		if(i == lA && i == lB) res = 0;
		else if(i == lB) res = 1;
		else res = -1;
	}
	delete [] vA;
	delete [] vB;
	return res;
}


int IndexManager::compareRID(const void* a, void* b, int& ridShift) const{
	/*
	rid shift is for varchar, represent after how many char, the rid is.
	*/

	RID_ATTR pgA, pgB, slotA, slotB;

	// pg
	memcpy(&pgA, (char *)a + ridShift, sizeof(RID_ATTR));
	memcpy(&pgB, (char *)b + ridShift, sizeof(RID_ATTR));
	if(pgA != pgB) return pgA > pgB ? 1 : -1;
	// slot
	memcpy(&slotA, (char *)a + ridShift + sizeof(RID_ATTR), sizeof(RID_ATTR));
	memcpy(&slotB, (char *)b + ridShift + sizeof(RID_ATTR), sizeof(RID_ATTR));
	if(slotA != slotB) return slotA > slotB ? 1 : -1;
	return 0;
}

// ==============================  IX_ScanIterator  ==============================


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

void IX_ScanIterator::init(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive)
{

	_ix = IndexManager::instance();
	_ixfileHandle = ixfileHandle;
	ifD = &ixfileHandle;
	_attribute = attribute;
    _lowKeyInclusive = lowKeyInclusive;
    _highKeyInclusive = highKeyInclusive;
    _tmpPage = NULL;

    // copy the value of lowKey and highKey
    _lowKey = copyKey(lowKey);
    _highKey = copyKey(highKey);
}

RID IX_ScanIterator::findLowBound(){
	void * page = malloc(PAGE_SIZE);
	RID rid;
	_ixfileHandle.readPage(0, page);
	// if _lowKeyInclusive is true and _lowKey exist, shift by 1 unit
	if(_lowKeyInclusive && _lowKey != NULL){
		if(_attribute.type == TypeInt){
			int tmp;
			memcpy(&tmp, (char *)_lowKey, sizeof(int));
			--tmp;
			memcpy((char *)_lowKey, &tmp, sizeof(int));
		}
		else if(_attribute.type == TypeReal){
			float tmp;
			memcpy(&tmp, (char *)_lowKey, sizeof(float));
			tmp -= 0.000004;
			// cout << "Modified low key is:" << tmp << endl;
			memcpy((char *)_lowKey, &tmp, sizeof(float));
		}
		else{
			int l;
			memcpy(&l, (char *)_lowKey, sizeof(int));
			char* tmp = new char[l];
			memcpy(tmp, (char *)_lowKey+sizeof(int), l);
			tmp[l-1] -= 1;
			memcpy((char *)_lowKey+sizeof(int), (char *)tmp, l);
			delete [] tmp;
		}
	}

	dfs_scan(page, 0, rid);

	// cout << "Low Bound is: " << rid.pageNum << ' ' << rid.slotNum << endl;

	_tmpPage = malloc(PAGE_SIZE);
	_ixfileHandle.readPage(rid.pageNum, _tmpPage);

	free(page);
	return rid;
}

void IX_ScanIterator::dfs_scan(void* page, int pageNum, RID& rid){
	// find recordSize, free_space, entry type
	int recordSize, free_space;
	_ixfileHandle.retrieveRecordsizeAndFreespace(page, recordSize, free_space);
	unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char)); // not sure
	bool isDataEntry = (isDataInt == 1 ? true : false);

	// _ixfileHandle.printPageInfo(page, pageNum);

	// bs, set last parameter(scanBS) to be true
	int id = _ix->binarySearch(_ixfileHandle, _lowKey, page, _attribute, isDataEntry, true);
	// cout << "Point At page: " << pageNum << ", " << id  << " slot." << endl;
	if(!isDataEntry){
		// access the position of slot and shift DATA_KEY_SIZE to point to pointer
		int pPage = _ix->findKeyAccessPage(_ixfileHandle, _attribute, id, page);
		void * pg = malloc(PAGE_SIZE);
		_ixfileHandle.readPage(pPage, pg);

		// recursion
		dfs_scan(pg, pPage, rid);
		free(pg);
	}
	else{
		// out of the limit of current page
		if(id > recordSize){
			memcpy(&rid.pageNum, (char *)page+FLAG_SIZE+LINK_SIZE, sizeof(int));
			rid.slotNum = 1;
		}
		else{
			rid.pageNum = pageNum;
			rid.slotNum = id;
		}
	}
}

void IX_ScanIterator::setRID(RID& rid){
	_itRID.pageNum = rid.pageNum;
	_itRID.slotNum = rid.slotNum;
}

void* IX_ScanIterator::copyKey(const void* key){
	if(key == NULL) return NULL;
	void* _k;
    if(_attribute.type == TypeVarChar){
    	int l;
    	memcpy(&l, (char *)key, sizeof(int));
    	_k = malloc(l + sizeof(int));
    	memcpy((char *)_k, (char *)key, l + sizeof(int));
    }
    else{
    	_k = malloc(sizeof(int));
    	memcpy((char *)_k, (char *)key, sizeof(int));
    }
    return _k;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    while(_itRID.pageNum != -1){  // DEFINE: last data entry page point to -1
        // search slot by slot)
		// if(!_tmpPage){
		// 	_tmpPage = malloc(PAGE_SIZE);
		// 	_ixfileHandle.readPage(_itRID.pageNum, _tmpPage);
		// }

        int record_num;
        memcpy(&record_num, (char *)_tmpPage + PAGE_SIZE - sizeof(int)*2, sizeof(int));
        while( _itRID.slotNum <= record_num){
        	// cout << "Iterate info: " << _itRID.pageNum << ' ' << _itRID.slotNum << endl;
        	// setting up output RID
        	int length = 0; // set reference for extractIthSlot()
        	void* slot = _ixfileHandle.extractIthSlot(_itRID.slotNum, _tmpPage, length);
        	memcpy((char *)key, (char *)slot, length - sizeof(RID_ATTR)*2);
        	//_ixfileHandle.printStr(slot, _attribute);
        	RID_ATTR pN;
        	RID_ATTR sN;
        	memcpy(&pN, (char *)slot + length - sizeof(RID_ATTR)*2 , sizeof(RID_ATTR));
        	memcpy(&sN, (char *)slot + length - sizeof(RID_ATTR), sizeof(RID_ATTR));
        	rid.pageNum = pN;
        	rid.slotNum = sN;

        	// every value is qualified
        	if(_highKey == NULL){
				++_itRID.slotNum;
				free(slot);
				return 0;
        	}

        	int res;
        	// compare
        	if(_attribute.type == TypeVarChar){
        		res = _ix->compareV(slot, _highKey);
        	}
        	else if(_attribute.type == TypeInt){
        		int vA, vB;
        		memcpy(&vA, (char *)slot, sizeof(int));
        		memcpy(&vB, (char *)_highKey, sizeof(int));
        		res = _ix->compareV(vA, vB);
        	}
        	else{
        		float vA, vB;
        		memcpy(&vA, (char *)slot, sizeof(float));
        		memcpy(&vB, (char *)_highKey, sizeof(float));
        		res = _ix->compareV(vA, vB);
        	}

        	free(slot);

    	    if(res > 0 || (res == 0 && !_highKeyInclusive)){
    	    	// transfer the counter value
    	        unsigned r, w, a;
    	        _ixfileHandle.collectCounterValues(r, w, a);
    	        ifD->setCounterValues(r, w, a);

    	    	return IX_EOF;
    	    }
    		else{
				++_itRID.slotNum;
				return 0;
    		}
        }
        // point to the next page
        int nextPg;
        memcpy(&nextPg, (char *)_tmpPage + FLAG_SIZE + LINK_SIZE, sizeof(int));
        _itRID.pageNum = nextPg;
        _itRID.slotNum = 1;
        _ixfileHandle.readPage(_itRID.pageNum, _tmpPage);

    }

    // transfer the counter value
    unsigned r, w, a;
    _ixfileHandle.collectCounterValues(r, w, a);
    ifD->setCounterValues(r, w, a);

    return IX_EOF;
}

RC IX_ScanIterator::close()
{
	if(_lowKey) free(_lowKey);
	if(_highKey) free(_highKey);
	if(_tmpPage) free(_tmpPage);
	//_ix->closeFile(_ixfileHandle);
    //fseek(_ixfileHandle.pfile, 0, SEEK_SET);
    _ixfileHandle.writePC();
    _ix->closeFile(_ixfileHandle);

	return 0;
}


// ==============================  IXFileHandle  ==============================


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    pfile = NULL;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::appendPage(const void *data)
{
    fseek(pfile, 0, SEEK_END);
    fwrite(data, sizeof(char), PAGE_SIZE, pfile);
    ixAppendPageCounter++;
    return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum >= ixAppendPageCounter){
        return -1;
    }
    // get the offset of the pageNum
    int offset = (pageNum+1) * PAGE_SIZE;
    fseek(pfile, offset, SEEK_SET);
    fread(data, sizeof(char), PAGE_SIZE, pfile);
    ixReadPageCounter++;
    return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= ixAppendPageCounter){
        return -1;
    }
    int offset = PAGE_SIZE * (pageNum+1);
    fseek(pfile, offset, SEEK_SET);
    fwrite(data, sizeof(char), PAGE_SIZE, pfile);
    ixWritePageCounter++;
    return 0;
}
RC IXFileHandle::deletePage(PageNum pageNum)
{
	if (pageNum >= ixAppendPageCounter){
        return -1;
    }
	void* headerPage = malloc(PAGE_SIZE);
	readHeader(headerPage);
	int recordSize, free_space;
	retrieveRecordsizeAndFreespace(headerPage, recordSize, free_space);
	
	// form the key entry
	//int pageNum = (int)pageNum;
	int pN;
	pN = (int)pageNum;

	// paste record and slot to page
	int insertOffset = PAGE_SIZE - free_space - sizeof(int)*2 - sizeof(int)*2*(recordSize);
	cout << free_space << ' ' << recordSize << endl;
	memcpy((char *)headerPage+insertOffset, &pN, sizeof(int));
	int slotOffset = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(recordSize+1);
	int slotSize = 4;
	memcpy((char *)headerPage+slotOffset, &insertOffset, sizeof(int));
	memcpy((char *)headerPage+slotOffset+sizeof(int),&slotSize, sizeof(int));

	
	// update recordSize, freespace, flag of header
	recordSize += 1;
	free_space -= 3*sizeof(int); // 4byte length, 4 byte offset, 4 byte data
	memcpy((char *)headerPage + PAGE_SIZE - sizeof(int)*2, &recordSize, sizeof(int));
	memcpy((char *)headerPage + PAGE_SIZE - sizeof(int), &free_space, sizeof(int) );

	writeHeader(headerPage);
	//free(headerPage);
}


RC IXFileHandle::readHeader(void *data)
{
    fseek(pfile, 0, SEEK_SET);
    fread(data, sizeof(char), PAGE_SIZE, pfile);
    ixReadPageCounter++;
    return 0;
}

RC IXFileHandle::writeHeader(const void *data)
{
    fseek(pfile, 0, SEEK_SET);
    fwrite(data, sizeof(char), PAGE_SIZE, pfile);
    ixWritePageCounter++;
    return 0;
}

unsigned IXFileHandle::getNumberOfPages()
{
    return ixAppendPageCounter;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

void IXFileHandle::createEmptyPage(void *data, bool isDataEntry, int lastPageId){
	// expect for free_space & number of slot, we also need to initialize flag and next pointing page.
    void* tmp_page = malloc(PAGE_SIZE);
	memset(tmp_page, 0, PAGE_SIZE);
    int initial_free_space = PAGE_SIZE-sizeof(int)*4-sizeof(char); // free space(4b), record number(4b), flag tag(1b), last page(4b), next page(4b),
    memcpy((char *)tmp_page+PAGE_SIZE - sizeof(int), &initial_free_space, sizeof(int));
    memcpy((char *)data, tmp_page, PAGE_SIZE);

    //flag: index:0/data:1
    unsigned char isDataInt = (isDataEntry ? 1 : 0);
    memcpy((char*)data, &isDataInt, sizeof(unsigned char));

    // last pointing page
    memcpy((char*)data + FLAG_SIZE, &lastPageId, sizeof(int));
    // next pointing page, init -1
    int next = -1;
    memcpy((char*)data + FLAG_SIZE + LINK_SIZE, &next, sizeof(int));
	free(tmp_page);
}

void IXFileHandle::retrieveRecordsizeAndFreespace(void* page, int& recordSize, int& free_space){
	memcpy(&recordSize, (char *)page + PAGE_SIZE - 2*sizeof(int) , sizeof(int));
	memcpy(&free_space, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
}

//void IXFileHandle::extractIthSlot()

// return length of slot
void* IXFileHandle::extractIthSlot(int i, void* page, int& length){
	// This function would key the entire entry in pages.
	// That is, if page is data entry, it gave
	// | key value | rid |  , 
	// if page is index, it gave 
	// | key value | rid | page pointer |
	int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
	int offset;
	memcpy(&offset, (char *)page + slotPosition, sizeof(int));
	memcpy(&length, (char *)page + slotPosition + sizeof(int), sizeof(int));
	void* slot = malloc(length);
	memcpy(slot, (char *)page + offset, length);

	int t;
	memcpy(&t, (char *)page + offset, sizeof(int));
	//cout << "offset is: " << offset <<  ", extract value at "<< i << " is: " << t << endl;
	return slot;
}

void* IXFileHandle::extractIthSlotDir(int i, void* page){
	void* slotDir = malloc(sizeof(int)*2);
	int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
	memcpy(slotDir, (char *)page + slotPosition, sizeof(int)*2);
	return slotDir;
}


void IXFileHandle::extractKey(int i, void* page, void* key, int& l, int &pointer){
	int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
	int offset, length;
	memcpy(&offset, (char *)page + slotPosition, sizeof(int));
	memcpy(&length, (char *)page + slotPosition + sizeof(int), sizeof(int));
	// cout << "yoeoe: " << offset << " " << length << endl;

	key = malloc(length-sizeof(int));
	memcpy(&key, (char *)page + offset, length-sizeof(int));
	// cout << "yoeoe" << endl;
	l = length - sizeof(int);
	memcpy(&pointer, (char *)page + offset + l, sizeof(int));
	// cout << "yoeoe" << endl;
	//return key;
}

void IXFileHandle::insertIthSlot(int id, const void* slot, void* page, bool isDataEntry, int KEY_SIZE){
	// determine key_size

	assert(id > 0);
	int key_size = isDataEntry ? KEY_SIZE : KEY_SIZE+PAGE_INFO;

    int recordSize, free_space;
    retrieveRecordsizeAndFreespace(page, recordSize, free_space);
	int insertSlotOffset = PAGE_SIZE - free_space - recordSize*sizeof(int)*2 - sizeof(int)*2;
	// if (insertSlotOffset < DATA_START){
	// 	// cout << "insert Slot Offset exceed. " << insertSlotOffset << endl; 

	// }
	memcpy((char *)page + insertSlotOffset, (char *)slot, key_size);

	int newFreeSpace = free_space - (sizeof(int)*2 + key_size);
	int newRecordSize = recordSize + 1;
	int keySz = key_size;

	//printInsertInfo(id, insertSlotOffset, keySz, newFreeSpace, newRecordSize, slot);


	memcpy((char *)page + PAGE_SIZE - sizeof(int)*2 - id*sizeof(int)*2, &insertSlotOffset, sizeof(int));

	memcpy((char *)page + PAGE_SIZE - sizeof(int)*2 - id*sizeof(int)*2 + sizeof(int), &keySz, sizeof(int));

	memcpy((char *)page + PAGE_SIZE - sizeof(int)*2, &newRecordSize, sizeof(int));

	memcpy((char *)page + PAGE_SIZE - sizeof(int), &newFreeSpace, sizeof(int));
}



void IXFileHandle::extractIthSlotKey(int i, void* key, void* page){
	// extract the key only, without page pointer
	int slotPosition = PAGE_SIZE - sizeof(int)*2 - sizeof(int)*2*(i);
	int offset, length;
	memcpy(&offset, (char *)page + slotPosition, sizeof(int));
	memcpy(&length, (char *)page + slotPosition + sizeof(int), sizeof(int));
	key = malloc(length);
	memcpy(&key, (char *)page + offset, length-KEY_TAIL);
}



void IXFileHandle::linkPage(void* page, void* splitPage, int& thisPageNum, int& splitPageNum){
	// extract next page of 'page'
	void* nxtPage = malloc(PAGE_SIZE);
	// find next page number
	int nxtPageNum;
	memcpy(&nxtPageNum, (char *)page + FLAG_SIZE + LINK_SIZE, sizeof(int));

	if(nxtPageNum >= 1)readPage(nxtPageNum, nxtPage); // avoid the case that nxtPageNum == -1 (no next page) and == 0(root page)

	// write to this page (point next to split page)
	memcpy((char *)page + FLAG_SIZE + LINK_SIZE, &splitPageNum, sizeof(int));

	// write to split page (point next to nxt page/ last to this page)
	memcpy((char *)splitPage + FLAG_SIZE, &thisPageNum, sizeof(int));
	memcpy((char *)splitPage + FLAG_SIZE + LINK_SIZE, &nxtPageNum, sizeof(int));

	if(nxtPageNum >= 1){
		// write to nxt page (point next to split page)
		memcpy((char *)nxtPage + FLAG_SIZE, &splitPageNum, sizeof(int));
		// only need to write nxtPage, thispage & splitPageNum would be modified later so we don't need to write back
		writePage(nxtPageNum, nxtPage);
	}

	free(nxtPage);


}

void IXFileHandle::createFirstData(void* page, int& pPage){
	void *newPage = malloc(PAGE_SIZE);
	createEmptyPage(newPage, true, -1);
	appendPage(newPage);

	pPage = getNumberOfPages()-1; // fixed
	memcpy((char *)page + DATA_START, &pPage, sizeof(int)); // put the page id in the 0st key of root page
	free(newPage);
}

void IXFileHandle::writePC(){
    fseek(pfile, 0, SEEK_SET);
    fwrite(&ixReadPageCounter, sizeof(int), 1, pfile);
    fwrite(&ixWritePageCounter, sizeof(int), 1, pfile);
    fwrite(&ixAppendPageCounter, sizeof(int), 1, pfile);
}

void IXFileHandle::readPC(){
    fseek(pfile, 0, SEEK_SET);
    fread(&ixReadPageCounter, sizeof(int), 1, pfile);
    fread(&ixWritePageCounter, sizeof(int), 1, pfile);
    fread(&ixAppendPageCounter, sizeof(int), 1, pfile);
}


RC IXFileHandle::setCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount){
	ixReadPageCounter = readPageCount;
	ixWritePageCounter = writePageCount;
	ixAppendPageCounter = appendPageCount;
    return 0;
}

// DEBUG function
void IXFileHandle::printPageInfo(void* page, int pageNum){
	int recordSize, free_space, lastPage, nextPage;
	retrieveRecordsizeAndFreespace(page, recordSize, free_space);
	unsigned char isDataInt;
	memcpy(&isDataInt, (char *)page, sizeof(unsigned char)); // not sure
	bool isDataEntry = (isDataInt == 1 ? true : false);
	memcpy(&lastPage, (char *)page+FLAG_SIZE, sizeof(int));
	memcpy(&nextPage, (char *)page+FLAG_SIZE+LINK_SIZE, sizeof(int));

	cout << "===== Page " << pageNum << " Info =====" << endl;
	if(isDataEntry) cout << "Page Type: DATA" << endl;
	else cout << "Page Type: KEY" << endl;
	cout << "Record Size: " << recordSize << endl;
	cout << "Free Space: " << free_space << endl;
	cout << "Last Page: " << lastPage << endl;
	cout << "Next Page: " << nextPage << endl;
	cout << "======================" << endl << endl;
}

void IXFileHandle::printInsertInfo(int& id, int& insertSlotOffset, int& keySz, int& newFreeSpace, int& newRecordSize, const void* key){
	int k;
	memcpy(&k, (char *)key, sizeof(int));
	cout << endl;
	cout << "@@@@@ Insert at: " << id << " Info @@@@@" << endl;
	cout << "Insert offset: " << insertSlotOffset << endl;
	cout << "Key Size: " << keySz << endl;
	cout << "Free Space: " << newFreeSpace << endl;
	cout << "Record Size: " << newRecordSize << endl;
	cout << "Value is: " << k << endl;
	cout << "@@@@@@@@@@@@@@@@@@@@" << endl << endl;
}

void IXFileHandle::printStr(const void* key, const Attribute &attribute){
	if(attribute.type != TypeVarChar) return;
	int l;
	memcpy(&l, (char *)key, sizeof(int));
	char* k = new char[l];
	memcpy((char *)k, (char *)key+ sizeof(int), l);

	for(int i = 0; i < l; ++i) cout << k[i];
	cout << endl;
	delete [] k;
}

