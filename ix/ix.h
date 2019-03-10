#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <climits>
#include <stdio.h>
#include <iostream>
#include <string.h>
#include <algorithm>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define FLAG_SIZE 1
# define LINK_SIZE 4
# define DATA_START 9
# define HEADER_START 12
# define KEY_TAIL 8 // fix overflow
# define PAGE_INFO 4
# define MIN_UTIL_RATIO 0.5 // the minimum utilized ratio of space 

typedef int RID_ATTR; // fix overflow


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {
    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void preOrderTraversal(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, int depth) const;
        
        RC dfs_delete(IXFileHandle &idxfileHandle, void *key, void* page, const Attribute &attribute, pair<void*, int>& childEntry, int pPage, 
                        void* parentPage, int id);
        void simpleDelete(IXFileHandle &ixfileHandle, void* page, int pageNum, int id, bool isDataEntry);

        void merge(IXFileHandle &ixfileHandle, const void *key, void* page, pair<void*, int>& childEntry, int pageNum, bool isDataEntry, void* parentPage,
                    int pageIdonParent);
        int redistribute(IXFileHandle &ixfileHandle, const void *key, pair<void*, int> &childEntry, void* page, int pageNum, void* sibling, int siblingNum, void* parent, int pageIdonParent, bool borrowFromRight);
        int naiveBinarySearch(IXFileHandle &ixfileHandle, const void *key, void* page, const Attribute &attribute, bool isDataEntry, bool scanBS);
        int binarySearch(IXFileHandle &ixfileHandle, const void *key, void* page, const Attribute &attribute, bool isDataEntry, bool scanBS);
        // Conduct depth first search while inserting
        void dfs_insert(IXFileHandle &ixfileHandle, const void *key, void* page, const Attribute &attribute, int& splitPage, int pPage);
        // Simply insert a slot in page
        void simpleInsert(IXFileHandle &ixfileHandle, const void *slot, void* page, int& splitPageId, int pageNum, int id, bool isDataEntry);
        // Split the page
        void split(IXFileHandle &ixfileHandle, const void *key, void* page, int& splitPageId, int pageNum, int id, bool isDataEntry);
        // Split the root page
        void splitFromRoot(IXFileHandle &ixfileHandle, const void *key, void* page, const Attribute &attribute, int& splitPageId, int pageNum, int id, bool isDataEntry);
        // Compact the data
        void compact(IXFileHandle &ixfileHandle, void* page, int lowSlot, int upSlot, void* newPage, int startOffset, int startSlot);
        void settingChildEntry(IXFileHandle &ixfileHandle, void* splitPage, pair<void*, int>& childEntry, int& splitPageNum, bool isDataEntry);
        // Shift the slot directory to make it order
        void shiftDir(IXFileHandle &ixfileHandle, void* page, int lowSlot, int upSlot, int startSlot);
        // Form slot of index page for insertion
        void* formIndexSlot(IXFileHandle &ixfileHandle, const Attribute &attribute, int& splitPage, int& keyLeng);
        void formIndexKey(void* slot, void* key, int pageNum, int length);
        // Initiate slot size
        void initSlotSize(int& slot_size);
        // Find which child page to point to given parent key page and id
        int findKeyAccessPage(IXFileHandle& ixfileHandle, const Attribute &attribute, int& id, void* page);
        int compare(const void* keyA, void* keyB, const Attribute &attribute, bool scanBS) const;
        int compareV(int& a, int& b) const;
        int compareV(float& a, float& b) const;
        int compareV(const void* a, const void* b) const;
        int compareRID(const void* a, void* b, int& ridShift) const;


        IndexManager();
        ~IndexManager();

    private:
        int SLOT_SIZE;
        int KEY_SIZE;
        RecordBasedFileManager *rbfm;
        static IndexManager *_index_manager;
};

class IXFileHandle {
public:
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    FILE *pfile;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC appendPage(const void *data);
    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, const void *data);
    RC deletePage(PageNum pageNum);
    RC readHeader(void *data);
    RC writeHeader(const void *data);
    unsigned getNumberOfPages();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
	void createEmptyPage(void *data, bool isDataEntry, int lastPageId);
	//void extractIthSlot(int i, void* slot, void* page);
    void extractKey(int i, void* page, void* key, int& l, int& pointer);

	void insertIthSlot(int i, const void* key, void* page, bool isDataEntry, int KEY_SIZE);
    void deleteIthSlot(int id, const void* key, void* page, bool isDataEntry, int KEY_SIZE);
    void extractIthSlotKey(int i, void* key, void* page);
	void* extractIthSlot(int i, void* page, int& leng);
	void* extractIthSlotDir(int i, void* page);
	void retrieveRecordsizeAndFreespace(void* page, int& recordSize, int& free_space);
	void createFirstData(void* page, int& pPage);
	void linkPage(void* page, void* splitPage, int& thisPageNum, int& splitPageNum);
	RC setCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    void readPC();
    void writePC();

    //debug
    void printPageInfo(void* page, int pageNum);
    void printInsertInfo(int& id, int& insertSlotOffset, int& keySz, int& newFreeSpace, int& newRecordSize, const void* key);
    void printStr(const void* key, const Attribute &attribute);
};


class IX_ScanIterator {
    public:
		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();



        void init(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey, const void *highKey, bool	lowKeyInclusive, bool highKeyInclusive);
        RID findLowBound();
        void dfs_scan(void* page, int pageNum, RID& rid);
        void setRID(RID& rid);
        void* copyKey(const void* key);
        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    private:
        IndexManager *_ix;
        IXFileHandle _ixfileHandle;
        IXFileHandle *ifD;
        RID _itRID; // iterating RID: use it to iterate data entry
        Attribute _attribute;
        void* _lowKey;
        void* _highKey;
        bool _lowKeyInclusive;
        bool _highKeyInclusive;
        void* _tmpPage;
};

#endif
