#include <iostream>
#include <fstream>
#include <vector>
#include <list>
#include <cstring>
#include <map>

#include <cstdio>
#include <cstdlib>
#include <sqlite3.h>

#include "helpers.hpp"

using namespace std;

/*
DOXS - 4 bytes
index_entries - 256*256*4 bytes ( each entry is named as a word )
  entries: list of words
    word:
      s : \0 terminated string
      stats_index: 4 byte offset to stats info

  stats_info:
    num_urls: 4 bytes
      each url has:
        index_to_url - 4 bytes
        frequency counter - 4 bytes

  url:
      url: \0 terminated string
      name: \0 terminated string

CREATE TABLE urlinfo(id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, name TEXT);
CREATE TABLE statsinfo(indexword_id INTEGER, urlinfo_id INTEGER, frequency INTEGER);
CREATE TABLE indexword(id INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT);

*/


// Global constants
const int header_length = 4;
const int num_index_entries =  256*256;
char default_search_index[] = "search.idx";
char default_output_db[] = "output_index.db";

// type declarations
typedef std::map<unsigned int, unsigned int, std::less<int> > URLMap;


void initialize() {
    sqlite3_initialize( );
}

void finalize() {
    sqlite3_shutdown( );
}

sqlite3* open_db(char *dbname){
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    if( !dbname) {
      dbname = default_output_db;
    }

    rc = sqlite3_open_v2( dbname, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL );
    if ( rc != SQLITE_OK) {
        cerr << "Can't open database: " <<  sqlite3_errmsg(db) << endl;
        sqlite3_close( db );
        exit( -1 );
    }


    const char *tables[] = {
      "DROP TABLE IF EXISTS urlinfo",
      "DROP TABLE IF EXISTS statsinfo",
      "DROP TABLE IF EXISTS indexword",
      "CREATE TABLE urlinfo(id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, name TEXT)",
      "CREATE TABLE statsinfo(indexword_id INTEGER, urlinfo_id INTEGER, frequency INTEGER)",
      "CREATE TABLE indexword(id INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT)"
    }
    ;

    sqlite3_stmt    *stmt = NULL;
    int num_queries = sizeof(tables)/sizeof(char *);
    for ( int i = 0 ; i < num_queries ; i++ ) {\
      const char * query = tables[i];
      rc = sqlite3_prepare_v2( db, query, -1, &stmt, NULL );
      if ( rc != SQLITE_OK) {
	cerr << query << " failed 1" << endl;
	exit( -1 );
      }
      
      rc = sqlite3_step( stmt );
      if ( rc != SQLITE_DONE ) {
	cerr << query << " failed 2" << endl;
	exit ( -1 );
      }

      sqlite3_finalize( stmt );
    }    

    return db;
}


int save_wordentry(sqlite3 *db, WordEntry &we) {
  sqlite3_stmt* stmt;
  int             rc = 0;
  unsigned int    row_id = 0;
  char insert_query[] = "INSERT INTO indexword(word) values(?);";
  
  sqlite3_prepare_v2(db,
		     insert_query,
		     strlen(insert_query), 
		     &stmt,
		     0);
  
  sqlite3_bind_text(stmt, 1,
		    we.getWord().c_str(),
		    strlen(we.getWord().c_str()),
		    SQLITE_STATIC
		    );
  
  rc = sqlite3_step(stmt);
  if (( rc != SQLITE_DONE )&&( rc != SQLITE_ROW )) {
    cerr << "save_wordentry(): failed 1" << endl;
    exit ( -1 );
  }
  sqlite3_finalize( stmt );

  char query[] = "select seq from sqlite_sequence where name='indexword'";
  rc = sqlite3_prepare_v2( db, query, strlen(query), &stmt, NULL );
  
  if ( rc != SQLITE_OK) {
    cerr << "save_wordentry(): failed 2" << endl;
    exit( -1 );
  }
  
  while( sqlite3_step( stmt ) == SQLITE_ROW ) {
    row_id = (unsigned int) sqlite3_column_int( stmt, 0 );
  }
  
  sqlite3_finalize( stmt );
  
  we.setId(row_id);

  return row_id;
}


void load_urlentry(sqlite3 *db, unsigned int id, URLEntry &ue) {
  
  sqlite3_stmt    *stmt = NULL;
  int             rc = 0;
  unsigned int    row_id = 0;

  char query[] = "select url, name from urlinfo where id=?";
  rc = sqlite3_prepare_v2( db, query, strlen(query), &stmt, NULL );

  sqlite3_bind_int(stmt, 1, id);
  
  if ( rc != SQLITE_OK) {
    cerr << "load_urlentry(): failed 1" << endl;
    exit( -1 );
  }
  
  while( sqlite3_step( stmt ) == SQLITE_ROW ) {
    ue.url = (const char *) sqlite3_column_text( stmt, 0 );
    ue.name = (const char *) sqlite3_column_text( stmt, 0 );
    ue.id = id;
  }
  
  sqlite3_finalize( stmt );
}

unsigned int save_urlentry(sqlite3 *db, URLEntry &ue) {
  
  sqlite3_stmt    *stmt = NULL;
  int             rc = 0;
  unsigned int    row_id = 0;
  

  char insert_query[] = "INSERT INTO urlinfo(url, name) values(?,?);";

  sqlite3_prepare_v2(db,
		     insert_query,
		     strlen(insert_query),
		     &stmt,
		     0);
  
  sqlite3_bind_text(stmt, 1, ue.url.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, ue.name.c_str(), -1, SQLITE_STATIC);
  rc = sqlite3_step(stmt);
  if (( rc != SQLITE_DONE )&&( rc != SQLITE_ROW )) {
    cerr << "save_urlentry(): failed 1" << endl;
    exit ( -1 );
  }
  sqlite3_finalize( stmt );
  

  char query[] = "select seq from sqlite_sequence where name='urlinfo'";
  rc = sqlite3_prepare_v2( db, query, strlen(query), &stmt, NULL );
  
  if ( rc != SQLITE_OK) {
    cerr << "save_urlentry(): failed 2" << endl;
    exit( -1 );
  }
  
  while( sqlite3_step( stmt ) == SQLITE_ROW ) {
    row_id = (unsigned int) sqlite3_column_int( stmt, 0 );
  }
  
  sqlite3_finalize( stmt );

  ue.id = row_id;

  return row_id;
}


void save_docinfo(sqlite3 *db, DocInfo &di) {
  
  sqlite3_stmt    *stmt = NULL;
  int             rc = 0;
  unsigned int    row_id = 0;
  

  char insert_query[] = "INSERT INTO statsinfo(indexword_id, urlinfo_id, frequency) values(?,?,?)";

  rc = sqlite3_prepare_v2(db,
		     insert_query,
		     strlen(insert_query),
		     &stmt,
		     0);

  if ( rc != SQLITE_OK ) {
    cerr << "save_docinfo(): failed 1" << endl;
    exit ( -1 );
  }
  
  sqlite3_bind_int( stmt, 1, di.indexword_id );
  sqlite3_bind_int( stmt, 2, di.urlinfo_id );
  sqlite3_bind_int( stmt, 3, di.getFreq() );

  rc = sqlite3_step(stmt);
  if (( rc != SQLITE_DONE )&&( rc != SQLITE_ROW )) {
    cerr << "save_docinfo(): failed 2" << endl;
    exit ( -1 );
  }
  sqlite3_finalize( stmt );

}

void read_index_entries(  ifstream &is, unsigned int *index_entries, int size) {
  is.seekg (4, ios::beg);
  // read all index entries
  for ( int i = 0 ; i<size; i ++ ) {
    unsigned int index;
    index = read_int(is);
    index_entries[i] = index;
  }
}


int main ( int argc, char * argv[]) {
  char header[header_length+1] = {0};
  char *search_index_file = 0;
  char *output_db_file = 0;
  URLMap url_map;


  cerr << "number of args: " << argc << endl;
  if(argc >= 2) {
    search_index_file = argv[1];
  } else {
    search_index_file= default_search_index;
  }

  if(argc >= 3) {
    output_db_file = argv[2];
  }

  initialize();

  sqlite3 *db = open_db(output_db_file);
  
  ifstream is;
  cerr << "file: " << search_index_file << endl;
  is.open (search_index_file, ios::binary);
  is.read (header, header_length);
  header[4] = '\0';
  cout << "File Header: [" <<  header << "]" << endl;

  int num_idx = 0;

  unsigned int index_entries[num_index_entries];
  read_index_entries(is, index_entries, num_index_entries);

  // iterate over all index entries
  for ( int i = 0 ; i<num_index_entries; i ++ ) {
    char s[1024] = {0};
    unsigned int index = index_entries[i];
    
    if(!index) {
      continue;
    }

    int count = 0;
    list<WordEntry *> word_entry_list;

    is.seekg (index, ios::beg);
    read_string(is, s);

    while (strlen(s)) {
      WordEntry *wp = new WordEntry;
      unsigned int stats_index = read_int(is);
      if (!count) {
        cout << index << endl;
	// break;
      }
      cout << "\t" << s << endl;
      cout << "\t" << stats_index << endl;

      wp->setWord(s);
      wp->setStatsIndex(stats_index);
      word_entry_list.push_back(wp);
      // clear the string
      s[0] = '\0';
      read_string(is, s);
      count ++;
    }


    list<WordEntry *>::iterator it;
    
    for(it = word_entry_list.begin();
	it != word_entry_list.end();
	it ++) {

      WordEntry *wp = *it;

      save_wordentry(db, *wp);

      // vector< DocInfo* > *di_list = new vector<DocInfo*>; 
      // wp->di_list = di_list;

      is.seekg(wp->getStatsIndex(), ios::beg);
      unsigned int num_docs = read_int(is);
      vector< DocInfo* > doc_info_list;

      for(int i = 0; i < num_docs; i++ ) {
	DocInfo *d = new DocInfo;
	unsigned int idx  = read_int(is);
	unsigned int freq = read_int(is);

	d->setIdx(idx);
	d->setFreq(freq);
	doc_info_list.push_back(d);
      }

      for(int i = 0; i < num_docs; i++ ) {
	DocInfo *d = doc_info_list[i];


	URLEntry ue;

	URLMap::iterator it = url_map.find(d->getIdx());

	// cerr << "::" << d->getIdx() << endl;

	unsigned int urlinfo_id = -1;

	if( it != url_map.end()) { // if we already have the url, load it

	  urlinfo_id = it->second;
	  load_urlentry(db, urlinfo_id, ue);
	  d->urlinfo_id = urlinfo_id;

	} else { // otherwise, fetch it and save

	  is.seekg(d->getIdx(), ios::beg);
	  s[0] = '\0'; read_string(is, s);
	  ue.name = s;
	  s[0] = '\0'; read_string(is, s);
	  ue.url = s;
	  s[0] = '\0';

	  urlinfo_id = save_urlentry(db, ue);
	  url_map[d->getIdx()] = urlinfo_id;

	  d->urlinfo_id = urlinfo_id;
	  d->indexword_id = wp->getId();
	}

	// save docinfo i.e. stats
	save_docinfo(db, *d);

	cout << "\t\t" << d->getIdx() 
	     << "::" << ue.id 
	     << "::" << urlinfo_id 
	     << endl;
	// cout << "\t\t" << d->getIdx() << endl;
	cout << "\t\t" << ue.name << endl;
	cout << "\t\t" << ue.url << endl;

      }

      // free all docinfo entries
      for(int i = 0; i < num_docs; i++ ) {
	DocInfo *d = doc_info_list[i];
	delete d;
      }
      
    }


    // free all word entries
    for(it = word_entry_list.begin();
	it != word_entry_list.end();
	it ++) {

      delete *it;
    }    

    if(count) {
      cout << "\t" << "number of entries: " << count << endl;
    }

  }

  cerr << "current position" << is.tellg() << endl;

  finalize();

  return 0;
}

