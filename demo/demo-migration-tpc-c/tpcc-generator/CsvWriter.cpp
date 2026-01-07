#include "CsvWriter.hpp"

#include <iomanip>
#include <cstring>

using namespace std;

namespace csv {

CsvWriter::CsvWriter(const string &path)
        : out(path)
          , firstWordInLine(true)
{
   if (!out.good()) {
      cout << "\nOh no, I can not create file: '" << path << "'." << std::endl;
      cout << "abooooooorting !!!!!!" << std::endl;
      exit(-1);
   }
}

void CsvWriter::prePrint()
{
   if (firstWordInLine) {
      firstWordInLine = false;
   } else {
      out << ',';
   }
}

CsvWriter &CsvWriter::printString(const char *str, int len)
{
   prePrint();
   out.write(str, std::min((size_t) len, strnlen(str, len)));
   return *this;
}

CsvWriter &operator<<(CsvWriter &csv, int64_t num)
{
   csv.prePrint();
   csv.out << num;
   return csv;
}

CsvWriter &operator<<(CsvWriter &csv, float num)
{
   csv.prePrint();
   csv.out << fixed << num;
   return csv;
}

CsvWriter &operator<<(CsvWriter &csv, const std::string &str)
{
   return csv.printString(str.c_str(), str.size());
}

CsvWriter &operator<<(CsvWriter &csv, EndlStruct)
{
   csv.out << "\n";
   csv.firstWordInLine = true;
   return csv;
}

CsvWriter &operator<<(CsvWriter &csv, Precision precision)
{
   csv.out << std::setprecision(precision.p);
   return csv;
}

}
