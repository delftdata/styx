
all: tpcc-generator

tpcc-generator: TpccGenerator.cpp TpccGenerator.hpp CsvWriter.cpp CsvWriter.hpp main.cpp
	g++ -o tpcc-generator TpccGenerator.cpp CsvWriter.cpp main.cpp -O3 -g0 -march=native -flto -std=c++11

clean:
	rm tpcc-generator
