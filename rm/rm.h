
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <climits>

#include "../ix/ix.h"
#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

typedef enum {Insert, Delete, Update} EntryOp;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  RBFM_ScanIterator rbfm_ScanIterator;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			// Terminate index scan
  IX_ScanIterator ix_ScanIterator;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);


// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

  int findIDbyTableName(const string &tableName);
  void findAttribyColumn(int& table_id, vector<Attribute>& attrs);
  void getTableAttributes(vector<Attribute> &attrs);
  void getColumnAttributes(vector<Attribute> &attrs);
  void getIndexAttributes(vector<Attribute> &attrs);
  void formEntryAndExec(const string &tableName, vector<Attribute> &attrs, const RID &rid, const void* record, EntryOp eOp); // form index entry and execute operation
  void initIdxwithData(const string &tableName, const string &attributeName);

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm_manager;
  RecordBasedFileManager *rbfm;
  IndexManager *ix;

  void createTableSlot(FileHandle &fileHandle, int table_id, const string table_name, const string file_name);
  void createColumnSlot(FileHandle &fileHandle, int table_id, const string column_name, int column_type, int column_length, int column_position);
  void createIndexSlot(FileHandle &fh, const string file_name, const string attribute_name);
};

#endif
