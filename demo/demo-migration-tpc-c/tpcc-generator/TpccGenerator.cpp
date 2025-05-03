#include "TpccGenerator.hpp"
#include "CsvWriter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cassert>
#include <csignal>
#include <cstring>
#include <utility>
#include <iomanip>
#include <sstream>
#include <unordered_set>

using namespace std;

TpccGenerator::TpccGenerator(const uint32_t warehouse_count, string folder)
        : warehouse_count(warehouse_count)
          , folder(std::move(folder))
          , ranny(42)
{
}

void TpccGenerator::generateItems() {
   cout << "Generating items .. " << flush;
   csv::CsvWriter i_csv(folder + "/item.csv");
   auto originalRows = selectUniqueIds(kItemCount / 10, 1, kItemCount);
   for (uint32_t i_id = 1; i_id<=kItemCount; i_id++) {
      generateItem(i_csv, i_id, originalRows);
   }
   cout << "ok !" << endl;
}


void TpccGenerator::generateItem(csv::CsvWriter& i_csv, const uint32_t i_id, unordered_set<uint32_t>& originalRows)
{
   const uint32_t i_im_id = makeNumber(MIN_IM, MAX_IM);
   array<char, MAX_I_NAME> i_name = {};
   makeAlphaString(MIN_I_NAME, MAX_I_NAME, i_name.data());
   const float i_price = fixedPoint(MONEY_DECIMALS,MIN_PRICE,MAX_PRICE);
   array<char, MAX_I_DATA> i_data = {};
   const uint32_t i_data_size = makeAlphaString(MIN_I_DATA, MAX_I_DATA, i_data.data());
   if (originalRows.find(i_id) != originalRows.end()) {
      const uint32_t pos = makeNumber(0L, i_data_size - 8);
      i_data[pos] = 'o';
      i_data[pos + 1] = 'r';
      i_data[pos + 2] = 'i';
      i_data[pos + 3] = 'g';
      i_data[pos + 4] = 'i';
      i_data[pos + 5] = 'n';
      i_data[pos + 6] = 'a';
      i_data[pos + 7] = 'l';
   }
   // @formatter:off
   i_csv << static_cast<int64_t>(i_id) << static_cast<int64_t>(i_im_id) << i_name
         << csv::Precision(2) << i_price << i_data << csv::endl;
   // @formatter:on
}

void TpccGenerator::generateWarehouses()
{
   cout << "Generating warehouse .. " << flush;
   csv::CsvWriter w_csv(folder + "/warehouse.csv");
   csv::CsvWriter d_csv(folder + "/district.csv");
   csv::CsvWriter c_csv(folder + "/customer.csv");
   csv::CsvWriter h_csv(folder + "/history.csv");
   csv::CsvWriter o_csv(folder + "/order.csv");
   csv::CsvWriter ol_csv(folder + "/order_line.csv");
   csv::CsvWriter no_csv(folder + "/new_order.csv");
   csv::CsvWriter s_csv(folder + "/stock.csv");


   for (uint32_t w_id = 1; w_id<=warehouse_count; w_id++) {
      generateWarehouse(w_csv, w_id);
      for (uint32_t d_id = 1; d_id<=kDistrictsPerWarehouse; d_id++) {
         uint32_t d_next_o_id = kCustomerPerDistrict + 1;
         generateDistrict(d_csv, w_id, d_id, d_next_o_id);

         std::vector<uint32_t> cIdPermutation;
         cIdPermutation.reserve(kCustomerPerDistrict);

         // Select 10% of the customers to have bad credit
         auto bad_customers = selectUniqueIds(kCustomerPerDistrict / 10, 1, kCustomerPerDistrict);
         for (uint32_t c_id = 1; c_id<=kCustomerPerDistrict; c_id++) {
            generateCustomer(c_csv, w_id, d_id, c_id, bad_customers);
            generateHistory(h_csv, w_id, d_id, c_id);
            cIdPermutation.push_back(c_id);
         }
         assert(cIdPermutation.front() == 1);
         assert(cIdPermutation.back() == kCustomerPerDistrict);
         std::shuffle(cIdPermutation.begin(), cIdPermutation.end(), ranny);

         for (uint32_t o_id = 1; o_id<=kCustomerPerDistrict; o_id++) {
            uint32_t o_ol_cnt = makeNumber(MIN_OL_CNT, MAX_OL_CNT);
            bool new_order = kCustomerPerDistrict - newOrdersPerDistrict < o_id;
            generateOrder(o_csv, w_id, d_id, o_id, cIdPermutation[o_id - 1], o_ol_cnt, new_order);
            for (uint32_t ol_number = 0; ol_number<o_ol_cnt; ol_number++) {
               generateOrderLine(ol_csv, w_id, d_id, o_id, ol_number, kItemCount, new_order);
            }
            if (new_order) {
               // @formatter:off
               no_csv << static_cast<int64_t>(o_id) << static_cast<int64_t>(d_id)
                      << static_cast<int64_t>(w_id) << csv::endl;
               // @formatter:on
            }
         }
      }
      // Select 10% of the stock to be marked "original"
      auto original_items = selectUniqueIds(kItemCount / 10, 1, kItemCount);
      for (int64_t i_id = 1; i_id<=kItemCount; i_id++) {
         generateStock(s_csv, w_id, i_id, original_items);
      }
   }
   cout << "ok !" << endl;
}

void TpccGenerator::generateWarehouse(csv::CsvWriter& w_csv, const uint32_t w_id) {
   const float w_tax = fixedPoint(TAX_DECIMALS,MIN_TAX,MAX_TAX);
   array<char, MAX_STREET> w_street_1 = {};
   array<char, MAX_STREET> w_street_2 = {};
   array<char, MAX_CITY> w_city = {};
   array<char, STATE> w_state = {};
   array<char, ZIP_LENGTH> w_zip = {};
   makeAddress(w_street_1.data(), w_street_2.data(), w_city.data(), w_state.data(), w_zip.data());
   array<char, MAX_NAME> w_name = {};
   makeAlphaString(MIN_NAME, MAX_NAME, w_name.data());
   // @formatter:off
   w_csv << static_cast<int64_t>(w_id) << w_name << w_street_1 << w_street_2 << w_city << w_state << w_zip << csv::Precision(TAX_DECIMALS)
         << w_tax << csv::Precision(INITIAL_YTD_DECIMALS) << INITIAL_W_YTD << csv::endl;
   // @formatter:on
}

void TpccGenerator::generateDistrict(csv::CsvWriter& d_csv, const uint32_t d_w_id, const uint32_t d_id, const uint32_t d_next_o_id)
{
   const float d_tax = fixedPoint(TAX_DECIMALS,MIN_TAX,MAX_TAX);
   array<char, MAX_NAME> d_name = {};
   makeAlphaString(MIN_NAME, MAX_NAME, d_name.data());
   array<char, MAX_STREET> d_street_1 = {};
   array<char, MAX_STREET> d_street_2 = {};
   array<char, MAX_CITY> d_city = {};
   array<char, STATE> d_state = {};
   array<char, ZIP_LENGTH> d_zip = {};
   makeAddress(d_street_1.data(), d_street_2.data(), d_city.data(), d_state.data(), d_zip.data());
   // @formatter:off
   d_csv << static_cast<int64_t>(d_id) << static_cast<int64_t>(d_w_id) << d_name << d_street_1 << d_street_2
         << d_city << d_state << d_zip << csv::Precision(TAX_DECIMALS) << d_tax
         << csv::Precision(INITIAL_YTD_DECIMALS) << INITIAL_D_YTD << static_cast<int64_t>(d_next_o_id) << csv::endl;
   // @formatter:on
}


void TpccGenerator::generateCustomer(csv::CsvWriter& c_csv, const uint32_t c_w_id, const uint32_t c_d_id, const uint32_t c_id, unordered_set<uint32_t>& bad_customers) {
   array<char, MAX_FIRST> c_first = {};
   makeAlphaString(MIN_FIRST, MAX_FIRST, c_first.data());
   assert(1 <= c_id && c_id <= kCustomerPerDistrict);
   array<char, MAX_LAST> c_last = {};
   if (c_id <= 1000) {
      makeLastName(c_id - 1, c_last.data());
   } else {
      makeRandomLastName(c_last.data());
   }
   array<char, PHONE> c_phone = {};
   makeNumberString(PHONE, PHONE, c_phone.data());
   array<char, DATETIME_SIZE> c_since = {};
   makeNow(c_since.data());
   array<char, 2> c_credit = {};
   if (bad_customers.find(c_id) != bad_customers.end()) {
      c_credit[0] = 'B';
   } else {
      c_credit[0] = 'G';
   }
   c_credit[1] = 'C';
   float c_discount = fixedPoint(DISCOUNT_DECIMALS,MIN_DISCOUNT,MAX_DISCOUNT);
   array<char, MAX_C_DATA> c_data = {};
   makeAlphaString(MIN_C_DATA, MAX_C_DATA, c_data.data());
   array<char, MAX_STREET> c_street_1 = {};
   array<char, MAX_STREET> c_street_2 = {};
   array<char, MAX_CITY> c_city = {};
   array<char, STATE> c_state = {};
   array<char, ZIP_LENGTH> c_zip = {};
   makeAddress(c_street_1.data(), c_street_2.data(), c_city.data(), c_state.data(), c_zip.data());
   // @formatter:off
   c_csv << static_cast<int64_t>(c_id) << static_cast<int64_t>(c_d_id) << static_cast<int64_t>(c_w_id) << c_first
         << C_MIDDLE << c_last << c_street_1 << c_street_2 << c_city
         << c_state << c_zip << c_phone << c_since << c_credit << csv::Precision(2) << INITIAL_CREDIT_LIM
         << csv::Precision(DISCOUNT_DECIMALS) << c_discount << csv::Precision(2) << INITIAL_BALANCE << INITIAL_YTD_PAYMENT
         << static_cast<int64_t>(INITIAL_PAYMENT_CNT) << static_cast<int64_t>(INITIAL_DELIVERY_CNT) << c_data << csv::endl;
   // @formatter:on
}

void TpccGenerator::generateHistory(csv::CsvWriter& h_csv, const uint32_t h_c_w_id, const uint32_t h_c_d_id, const uint32_t h_c_id) {
   array<char, DATETIME_SIZE> h_date = {};
   makeNow(h_date.data());
   array<char, MAX_DATA> h_data = {};
   makeAlphaString(MIN_DATA, MAX_DATA, h_data.data());
   // @formatter:off
   h_csv << static_cast<int64_t>(h_c_id) << static_cast<int64_t>(h_c_d_id) << static_cast<int64_t>(h_c_w_id)
         << static_cast<int64_t>(h_c_d_id) << static_cast<int64_t>(h_c_w_id) << h_date << INITIAL_AMOUNT << h_data << csv::endl;
   // @formatter:on
}

void TpccGenerator::generateStock(csv::CsvWriter& s_csv, const uint32_t s_w_id, const uint32_t s_i_id, unordered_set<uint32_t>& original_items)
{
   const int64_t s_quantity = makeNumber(MIN_QUANTITY, MAX_QUANTITY);
   array<char, MAX_I_DATA> s_data = {};
   const uint32_t s_data_size = makeAlphaString(MIN_I_DATA, MAX_I_DATA, s_data.data());
   if (original_items.find(s_i_id) != original_items.end()) {
      const int64_t pos = makeNumber(0L, s_data_size - 8);
      s_data[pos] = 'o';
      s_data[pos + 1] = 'r';
      s_data[pos + 2] = 'i';
      s_data[pos + 3] = 'g';
      s_data[pos + 4] = 'i';
      s_data[pos + 5] = 'n';
      s_data[pos + 6] = 'a';
      s_data[pos + 7] = 'l';
   }

   array<char, DIST> s_dist_01 = {};
   array<char, DIST> s_dist_02 = {};
   array<char, DIST> s_dist_03 = {};
   array<char, DIST> s_dist_04 = {};
   array<char, DIST> s_dist_05 = {};
   array<char, DIST> s_dist_06 = {};
   array<char, DIST> s_dist_07 = {};
   array<char, DIST> s_dist_08 = {};
   array<char, DIST> s_dist_09 = {};
   array<char, DIST> s_dist_10 = {};
   makeAlphaString(DIST, DIST, s_dist_01.data());
   makeAlphaString(DIST, DIST, s_dist_02.data());
   makeAlphaString(DIST, DIST, s_dist_03.data());
   makeAlphaString(DIST, DIST, s_dist_04.data());
   makeAlphaString(DIST, DIST, s_dist_05.data());
   makeAlphaString(DIST, DIST, s_dist_06.data());
   makeAlphaString(DIST, DIST, s_dist_07.data());
   makeAlphaString(DIST, DIST, s_dist_08.data());
   makeAlphaString(DIST, DIST, s_dist_09.data());
   makeAlphaString(DIST, DIST, s_dist_10.data());

   // @formatter:off
   s_csv << static_cast<int64_t>(s_i_id) << static_cast<int64_t>(s_w_id) << s_quantity
         << s_dist_01 << s_dist_02 << s_dist_03 << s_dist_04 << s_dist_05 << s_dist_06 << s_dist_07 << s_dist_08 << s_dist_09 << s_dist_10
         << static_cast<int64_t>(0) << static_cast<int64_t>(0) << static_cast<int64_t>(0) << s_data << csv::endl;
   // @formatter:on
}

void TpccGenerator::generateOrder(csv::CsvWriter& o_csv, const uint32_t o_w_id, const uint32_t o_d_id, const uint32_t o_id, const uint32_t o_c_id, const uint32_t o_ol_cnt, const bool new_order)
{
   array<char, DATETIME_SIZE> o_entry_d = {};
   makeNow(o_entry_d.data());
   const int64_t o_carrier_id = makeNumber(MIN_CARRIER_ID, MAX_CARRIER_ID);
   const int64_t o_all_local = 1;
   // @formatter:off
   o_csv << static_cast<int64_t>(o_id) << static_cast<int64_t>(o_d_id) << static_cast<int64_t>(o_w_id) << static_cast<int64_t>(o_c_id)
         << o_entry_d << (new_order ? "null" : to_string(o_carrier_id))
         << static_cast<int64_t>(o_ol_cnt) << o_all_local << csv::endl;
   // @formatter:on
}

void TpccGenerator::generateOrderLine(csv::CsvWriter& ol_csv, const uint32_t ol_w_id, const uint32_t ol_d_id, const uint32_t ol_o_id, const uint32_t ol_number, const uint32_t max_items, const bool new_order)
{
   const int64_t ol_i_id = makeNumber(1, max_items);
   int64_t ol_supply_w_id = ol_w_id;
   array<char, DATETIME_SIZE> ol_delivery_d = {};
   makeNow(ol_delivery_d.data());
   // 1% of items are from a remote warehouse
   const bool remote = makeNumber(1, 100) == 1;
   if (warehouse_count > 1 && remote) {
      ol_supply_w_id = makeNumberExcluding(1, warehouse_count, ol_w_id);
   }
   array<char, DIST> ol_dist_info = {};
   makeAlphaString(DIST, DIST, ol_dist_info.data());
   float ol_amount;
   if (new_order) {
      ol_amount = fixedPoint(MONEY_DECIMALS,MIN_AMOUNT,MAX_PRICE);
      // @formatter:off
      ol_csv << static_cast<int64_t>(ol_o_id) << static_cast<int64_t>(ol_d_id) << static_cast<int64_t>(ol_w_id)
             << static_cast<int64_t>(ol_number) << ol_i_id << ol_supply_w_id << "null"
             << static_cast<int64_t>(INITIAL_QUANTITY) << csv::Precision(2) << ol_amount << ol_dist_info << csv::endl;
      // @formatter:on
   } else {
      ol_amount = 0.0f;
      // @formatter:off
      ol_csv << static_cast<int64_t>(ol_o_id) << static_cast<int64_t>(ol_d_id) << static_cast<int64_t>(ol_w_id)
             << static_cast<int64_t>(ol_number) << ol_i_id << ol_supply_w_id << ol_delivery_d
             << static_cast<int64_t>(INITIAL_QUANTITY) << csv::Precision(2) << ol_amount << ol_dist_info << csv::endl;
      // @formatter:on
   }
}

uint32_t TpccGenerator::makeAlphaString(const uint32_t min, const uint32_t max, char *dest)
{
   const uint32_t len = makeNumber(min, max);
   for (uint32_t i = 0; i < len; i++) {
      dest[i] = static_cast<char>('a' + makeNumber(0, 25));
   }
   dest[len] = '\0';
   return len;
}

uint32_t TpccGenerator::makeNumberString(const uint32_t min, const uint32_t max, char *dest)
{
   const uint32_t len = makeNumber(min, max);
   for (uint32_t i = 0; i < len; i++) {
      dest[i] = static_cast<char>('0' + makeNumber(0, 9));
   }
   dest[len] = '\0';
   return len;
}

void TpccGenerator::makeAddress(char *street1, char *street2, char *city, char *state, char *zip)
{
   makeAlphaString(MIN_STREET, MAX_STREET, street1);
   makeAlphaString(MIN_STREET, MAX_STREET, street2);
   makeAlphaString(MIN_CITY, MAX_CITY, city);
   makeAlphaString(STATE, STATE, state);
   const uint32_t zip_length = ZIP_LENGTH - static_cast<uint32_t>(ZIP_SUFFIX.length());
   makeNumberString(zip_length, zip_length, zip);
   for (uint32_t i = zip_length; i<ZIP_LENGTH; i++) {
      zip[i] = ZIP_SUFFIX[i - zip_length];
   }
}

float TpccGenerator::fixedPoint(const uint32_t decimal_places, const float min, const float max)
{
   assert(decimal_places > 0);
   assert(max > min);

   const auto multiplier = static_cast<uint32_t>(std::pow(10, decimal_places));

   const auto scaled_min = static_cast<uint32_t>(std::lround(min * static_cast<float>(multiplier)));
   const auto scaled_max = static_cast<uint32_t>(std::lround(max * static_cast<float>(multiplier)));

   const uint32_t random_scaled = makeNumber(scaled_min, scaled_max);
   return static_cast<float>(random_scaled) / static_cast<float>(multiplier);
}

uint32_t TpccGenerator::makeNumber(const uint32_t min, const uint32_t max)
{
   std::uniform_int_distribution<uint32_t> distrib(min, max);
   return distrib(ranny);
}

uint32_t TpccGenerator::makeNumberExcluding(uint32_t minimum, const uint32_t maximum, const uint32_t excluding)
{
   assert(minimum < maximum);
   assert(minimum <= excluding && excluding <= maximum);
   uint32_t num = makeNumber(minimum, maximum - 1);
   if (num >= excluding) {
      num += 1;
   }
   assert(minimum <= num && num <= maximum && num != excluding);
   return num;
}

vector<uint32_t> TpccGenerator::makePermutation(const uint32_t min, const uint32_t max)
{
   assert(max > min);
   const uint32_t count = max - min;
   vector<uint32_t> result(count);
   iota(result.begin(), result.end(), min);

   for (uint32_t i = 0; i < count; i++) {
      swap(result[i], result[makeNumber(0, count - 1)]);
   }
   return result;
}

void TpccGenerator::makeLastName(const int64_t num, char *name)
{
   // A last name as defined by TPC-C 4.3.2.3. Not actually random.
   static const char *n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   strcpy(name, n[num / 100]);
   strcat(name, n[(num / 10) % 10]);
   strcat(name, n[num % 10]);
}

void TpccGenerator::makeRandomLastName(char *name)
{
   // A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be limited to maxCID.
   const uint32_t number = NURand(255, 0, 999);
   makeLastName(number, name);
}

uint32_t TpccGenerator::NURand(const uint32_t a, const uint32_t x, const uint32_t y) {
   uint32_t c;
   if (a == 255) {
      c = makeNumber(0, 255);
   }else if (a == 1023) {
      c = makeNumber(0, 1023);
   }
   else if (a == 8191) {
      c = makeNumber(0, 8191);
   }
   else {
      exit(-1);
   }
   return ((makeNumber(0, a) | makeNumber(x, y)) + c) % (y - x + 1) + x;
}


void TpccGenerator::makeNow(char *str)
{
   using namespace std::chrono;

   // Get current time
   const auto now = system_clock::now();
   const auto now_time_t = system_clock::to_time_t(now);
   const auto now_us = duration_cast<microseconds>(now.time_since_epoch()) % 1'000'000;

   // Format time into a string
   std::tm tm{};
   localtime_r(&now_time_t, &tm);

   std::ostringstream oss;
   oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
   oss << "." << std::setfill('0') << std::setw(6) << now_us.count(); // microseconds

   const std::string s = oss.str();
   std::strncpy(str, s.c_str(), s.size());
}

std::unordered_set<uint32_t> TpccGenerator::selectUniqueIds(const uint32_t numUnique, const uint32_t minimum, const uint32_t maximum)
{
   std::unordered_set<uint32_t> rows;
   while (rows.size() < numUnique) {
      uint32_t index = makeNumber(minimum, maximum);
      rows.insert(index);  // unordered_set automatically ignores duplicates
   }
   assert(rows.size() == numUnique);
   return rows;
}
