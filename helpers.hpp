#include <iostream>
#include <fstream>

using namespace std;

class URLEntry {

public:
  unsigned int id;
  string url;
  string name;

  URLEntry() {
    id = 0;
    url = "";
    name = "";
  }
}
  ;

class DocInfo {
  unsigned int idx;
  unsigned int freq;
  unsigned int rank;
  unsigned int hi;

public:
  unsigned int indexword_id;
  unsigned int urlinfo_id;
  // unsigned int frequency;

  unsigned int getIdx() { return idx; }
  void setIdx(unsigned int i) { idx = i; }
  unsigned int getFreq() { return freq; }
  void setFreq(unsigned int i) { freq = i; }
  unsigned int getRank() { return 0; }
  // void setRank(unsigned int i) { rank = i; }
  unsigned int getHi() { return freq & 1; }
  // void setHi(unsigned int i) { hi = i; }
}
  ;


class WordEntry {
  unsigned int id;
  string word;
  unsigned int stats_index;
public:
  vector<DocInfo*> *di_list;
  void setId(unsigned int i) { id = i; }
  unsigned int getId() { return id; }
  void setWord(char *s) {
    word = s;
  }

  void setStatsIndex(unsigned int i) {
    stats_index = i;
  }

  string getWord() {
    return word;
  }

  unsigned int getStatsIndex() {
    return stats_index;
  }
}
  ;




void read_string(ifstream& is, char p[]) {
  int i = 0;
  char c;
  while ( (c = is.get()) != -1 ) {
    char k = static_cast<char>(c);
    if(k == '\0') {
      break;
    } else {
      p[i] = c;
      i++;
    }
  }

  p[i] = '\0';
}

unsigned int read_int(ifstream &is){
    unsigned int idx;
    unsigned char b1, b2, b3, b4;

    b1 = (unsigned char) is.get();
    b2 = (unsigned char) is.get();
    b3 = (unsigned char) is.get();
    b4 = (unsigned char) is.get();

    idx = 
      ((static_cast<unsigned int>(b1))<<24) | 
      ((static_cast<unsigned int>(b2))<<16) |
      ((static_cast<unsigned int>(b3))<< 8) |
      (static_cast<unsigned int>(b4));

    return idx;
}


