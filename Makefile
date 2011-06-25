all:
	g++ -ggdb -c read_index.cpp -o read_index.o
	g++ -ggdb read_index.o -o read_index `pkg-config sqlite3 --cflags --libs`

