#pragma once

#include <iostream>
#include <array>
#include <fstream>

namespace csv {

struct Precision {
   Precision(int p)
           : p(p) {}
   int p;
};

static struct EndlStruct { // Not std way to it but really easy for here.
} endl;

class CsvWriter {
   std::ofstream out;
   bool firstWordInLine;

   void prePrint();
   CsvWriter &printString(const char *str, int len);
public:
   CsvWriter(const std::string &path);

   friend CsvWriter &operator<<(CsvWriter &csv, int64_t num);
   friend CsvWriter &operator<<(CsvWriter &csv, float num);
   template<unsigned long len>
   friend CsvWriter &operator<<(CsvWriter &csv, const std::array<char, len> &data) { return csv.printString(data.data(), len); }
   friend CsvWriter &operator<<(CsvWriter &csv, const std::string &str);
   friend CsvWriter &operator<<(CsvWriter &csv, EndlStruct);
   friend CsvWriter &operator<<(CsvWriter &csv, Precision);
};

}
