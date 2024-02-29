#include "TpccGenerator.hpp"
#include "CsvWriter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cassert>
#include <cstring>

using namespace std;

TpccGenerator::TpccGenerator(int64_t warehouse_count, const string &folder)
        : warehouse_count(warehouse_count)
          , folder(folder)
          , ranny(42)
{
}

void TpccGenerator::generateWarehouses()
{
   cout << "Generating warehouse .. " << flush;

   int64_t w_id;
   array<char, 10> w_name = {};
   array<char, 20> w_street_1 = {};
   array<char, 20> w_street_2 = {};
   array<char, 20> w_city = {};
   array<char, 2> w_state = {};
   array<char, 9> w_zip = {};
   float w_tax;
   float w_ytd;

   csv::CsvWriter w_csv(folder + "/warehouse.csv");

   for (w_id = 1L; w_id<=warehouse_count; w_id++) {
      makeAlphaString(6, 10, w_name.data());
      makeAddress(w_street_1.data(), w_street_2.data(), w_city.data(), w_state.data(), w_zip.data());
      w_tax = ((float) makeNumber(10L, 20L)) / 100.0f;
      w_ytd = 3000000.00f;

      // @formatter:off
      w_csv << w_id << w_name << w_street_1 << w_street_2 << w_city << w_state << w_zip << csv::Precision(4)
            << w_tax << csv::Precision(2) << w_ytd << csv::endl;
      // @formatter:on
   }

   cout << "ok !" << endl;
}

void TpccGenerator::generateDistricts()
{
   cout << "Generating districts .. " << flush;

   int64_t d_id;
   int64_t d_w_id;
   array<char, 10> d_name = {};
   array<char, 20> d_street_1 = {};
   array<char, 20> d_street_2 = {};
   array<char, 20> d_city = {};
   array<char, 2> d_state = {};
   array<char, 9> d_zip = {};
   float d_tax;
   float d_ytd;
   int64_t d_next_o_id;

   csv::CsvWriter d_csv(folder + "/district.csv");

   // Each warehouse has DIST_PER_WARE (10) districts
   for (d_w_id = 1; d_w_id<=warehouse_count; d_w_id++) {
      for (d_id = 1; d_id<=kDistrictsPerWarehouse; d_id++) {
         d_ytd = 30000.0;
         d_next_o_id = 3001L;
         makeAlphaString(6L, 10L, d_name.data());
         makeAddress(d_street_1.data(), d_street_2.data(), d_city.data(), d_state.data(), d_zip.data());
         d_tax = ((float) makeNumber(10L, 20L)) / 100.0f;

         // @formatter:off
         d_csv << d_id << d_w_id << d_name << d_street_1 << d_street_2 << d_city << d_state << d_zip << csv::Precision(4)
               << d_tax << csv::Precision(2) << d_ytd << d_next_o_id << csv::endl;
         // @formatter:on
      }
   }

   cout << "ok !" << endl;
}

void TpccGenerator::generateCustomerAndHistory()
{
   cout << "Generating customers and their history .. " << flush;

   int64_t c_id;
   int64_t c_d_id;
   int64_t c_w_id;
   array<char, 16> c_first = {};
   array<char, 2> c_middle = {};
   array<char, 16> c_last = {};
   array<char, 20> c_street_1 = {};
   array<char, 20> c_street_2 = {};
   array<char, 20> c_city = {};
   array<char, 2> c_state = {};
   array<char, 9> c_zip = {};
   array<char, 16> c_phone = {};
   array<char, 15> c_since = {}; // XXX used in history and customer and generated over and over again
   array<char, 2> c_credit = {};
   float c_credit_lim;
   float c_discount;
   float c_balance;
   array<char, 500> c_data = {};
   float h_amount;
   array<char, 24> h_data = {};

   csv::CsvWriter c_csv(folder + "/customer.csv");
   csv::CsvWriter h_csv(folder + "/history.csv");

   // Each warehouse has DIST_PER_WARE (10) districts
   for (c_w_id = 1; c_w_id<=warehouse_count; c_w_id++) {
      for (c_d_id = 1; c_d_id<=kDistrictsPerWarehouse; c_d_id++) {
         for (c_id = 1; c_id<=kCustomerPerDistrict; c_id++) {
            makeAlphaString(8, 16, c_first.data());
            c_middle[0] = 'O';
            c_middle[1] = 'E';
            if (c_id<=1000)
               makeLastName(c_id - 1, c_last.data());
            else
               makeLastName(makeNonUniformRandom(255, 0, 999), c_last.data());
            makeAddress(c_street_1.data(), c_street_2.data(), c_city.data(), c_state.data(), c_zip.data());
            makeNumberString(16, 16, c_phone.data());
            c_credit[0] = makeNumber(0L, 1L) == 0 ? 'G' : 'B';
            c_credit[1] = 'C';
            c_credit_lim = 50000;
            c_discount = ((float) makeNumber(0L, 50L)) / 100.0f;
            c_balance = -10.0f;
            makeNow(c_since.data());
            makeAlphaString(300, 500, c_data.data());

            // @formatter:off
            c_csv << c_id << c_d_id << c_w_id << c_first << c_middle << c_last << c_street_1 << c_street_2 << c_city
                  << c_state << c_zip << c_phone << c_since << c_credit << csv::Precision(2) << c_credit_lim
                  << csv::Precision(4) << c_discount << csv::Precision(2) << c_balance << 10.0f << int64_t(1)
                  << int64_t(0) << c_data << csv::endl;
            // @formatter:on

            h_amount = 10.0;
            makeAlphaString(12, 24, h_data.data());

            // @formatter:off
            h_csv << c_id << c_d_id << c_w_id << c_d_id << c_w_id << c_since << h_amount << h_data << csv::endl;
            // @formatter:on
         }
      }
   }

   cout << "ok !" << endl;
}

void TpccGenerator::generateItems()
{
   cout << "Generating items .. " << flush;

   int64_t i_id;
   array<char, 24> i_name = {};
   float i_price;
   array<char, 50> i_data = {};
   uint32_t i_data_size;
   vector<bool> orig(kItemCount, false);
   int64_t i_im_id;

   csv::CsvWriter i_csv(folder + "/item.csv");

   for (uint32_t i = 0; i<kItemCount / 10; i++) {
      uint32_t pos;
      do {
         pos = makeNumber(0L, kItemCount - 1);
      } while (orig[pos]);
      orig[pos] = true;
   }

   for (i_id = 1; i_id<=kItemCount; i_id++) {
      makeAlphaString(14, 24, i_name.data());
      i_price = ((float) makeNumber(100L, 10000L)) / 100.0f;
      i_data_size = makeAlphaString(26, 50, i_data.data());
      i_im_id = makeNumber(0, 10000);
      if (orig[i_id]) {
         uint32_t pos = makeNumber(0L, i_data_size - 8);
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
      i_csv << i_id << i_im_id << i_name << csv::Precision(2) << i_price << i_data << csv::endl;
      // @formatter:on
   }

   cout << "ok !" << endl;
}

void TpccGenerator::generateStock()
{
   cout << "Generating stocks .. " << flush;

   int64_t s_i_id;
   int64_t s_w_id;
   int64_t s_quantity;
   array<char, 24> s_dist_01 = {};
   array<char, 24> s_dist_02 = {};
   array<char, 24> s_dist_03 = {};
   array<char, 24> s_dist_04 = {};
   array<char, 24> s_dist_05 = {};
   array<char, 24> s_dist_06 = {};
   array<char, 24> s_dist_07 = {};
   array<char, 24> s_dist_08 = {};
   array<char, 24> s_dist_09 = {};
   array<char, 24> s_dist_10 = {};
   int64_t s_ytd = 0;
   int64_t s_order_cnt = 0;
   int64_t s_remote_cnt = 0;
   array<char, 50> s_data = {};
   vector<bool> orig(kItemCount, false);

   csv::CsvWriter s_csv(folder + "/stock.csv");

   for (s_w_id = 1; s_w_id<=warehouse_count; s_w_id++) {
      orig.assign(kItemCount, false);

      for (uint32_t i = 0; i<kItemCount / 10; i++) {
         int64_t pos;
         do {
            pos = makeNumber(0L, kItemCount - 1);
         } while (orig[pos]);
         orig[pos] = 1;
      }

      for (s_i_id = 1; s_i_id<=kItemCount; s_i_id++) {
         s_quantity = makeNumber(10L, 100L);
         makeAlphaString(24, 24, s_dist_01.data());
         makeAlphaString(24, 24, s_dist_02.data());
         makeAlphaString(24, 24, s_dist_03.data());
         makeAlphaString(24, 24, s_dist_04.data());
         makeAlphaString(24, 24, s_dist_05.data());
         makeAlphaString(24, 24, s_dist_06.data());
         makeAlphaString(24, 24, s_dist_07.data());
         makeAlphaString(24, 24, s_dist_08.data());
         makeAlphaString(24, 24, s_dist_09.data());
         makeAlphaString(24, 24, s_dist_10.data());
         uint32_t s_data_size = makeAlphaString(26, 50, s_data.data());
         if (orig[s_i_id]) {
            int64_t pos = makeNumber(0L, s_data_size - 8);
            s_data[pos] = 'o';
            s_data[pos + 1] = 'r';
            s_data[pos + 2] = 'i';
            s_data[pos + 3] = 'g';
            s_data[pos + 4] = 'i';
            s_data[pos + 5] = 'n';
            s_data[pos + 6] = 'a';
            s_data[pos + 7] = 'l';
         }

         // @formatter:off
         s_csv << s_i_id << s_w_id << s_quantity << s_dist_01 << s_dist_02 << s_dist_03 << s_dist_04 << s_dist_05
               << s_dist_06 << s_dist_07 << s_dist_08 << s_dist_09 << s_dist_10 << s_ytd << s_order_cnt
               << s_remote_cnt << s_data << csv::endl;
         // @formatter:on
      }
   }

   cout << "ok !" << endl;
}

void TpccGenerator::generateOrdersAndOrderLines()
{
   cout << "Generating orders .. " << flush;

   int64_t o_id;
   int64_t o_c_id;
   int64_t o_d_id;
   int64_t o_w_id;
   int64_t o_carrier_id;
   int64_t o_ol_cnt;
   array<char, 15> o_entry_d = {}; // XXX not sure if date is generate correctly
   int64_t o_all_local = 1;

   int64_t ol_number;
   int64_t ol_i_id;
   int64_t ol_quantity;
   float ol_amount;
   array<char, 24> ol_dist_info = {};
   string kNull = "null";

   csv::CsvWriter o_csv(folder + "/order.csv");
   csv::CsvWriter ol_csv(folder + "/order_line.csv");
   csv::CsvWriter no_csv(folder + "/new_order.csv");

   // Generate ORD_PER_DIST (3000) orders and order line items for each district
   for (o_w_id = 1L; o_w_id<=warehouse_count; o_w_id++) {
      for (o_d_id = 1L; o_d_id<=kDistrictsPerWarehouse; o_d_id++) {
         // Each customer has exactly one order
         vector<uint32_t> customer_id_permutation = makePermutation(1, kCustomerPerDistrict + 1);

         for (o_id = 1; o_id<=OrdersPerDistrict; o_id++) {
            o_c_id = customer_id_permutation[o_id - 1];
            o_carrier_id = makeNumber(1L, 10L);
            o_ol_cnt = makeNumber(5L, 15L);
            makeNow(o_entry_d.data());

            // @formatter:off
            o_csv << o_id << o_d_id << o_w_id << o_c_id << o_entry_d << (o_id>2100 ? kNull : to_string(o_carrier_id))
                  << o_ol_cnt << o_all_local << csv::endl;
            // @formatter:on

            // Order line items
            for (ol_number = 1; ol_number<=o_ol_cnt; ol_number++) {
               ol_i_id = makeNumber(1L, kItemCount);
               ol_quantity = 5;
               makeAlphaString(24, 24, ol_dist_info.data());

               if (o_id>2100) {
                  ol_amount = (float) (makeNumber(10L, 10000L)) / 100.0f;
                  // @formatter:off
                  ol_csv << o_id << o_d_id << o_w_id << ol_number << ol_i_id << o_w_id << kNull
                         << ol_quantity << csv::Precision(2) << ol_amount << ol_dist_info << csv::endl;
                  // @formatter:on
               } else {
                  ol_amount = 0.0f;
                  // @formatter:off
                  ol_csv << o_id << o_d_id << o_w_id << ol_number << ol_i_id << o_w_id
                         << o_entry_d.data() << ol_quantity << csv::Precision(2) << ol_amount << ol_dist_info << csv::endl;
                  // @formatter:on
               }
            }

            // Generate a new order entry for the order for the last 900 rows
            if (o_id>2100) {
               no_csv << o_id << o_d_id << o_w_id << csv::endl;
            }
         }
      }
   }

   cout << "ok !" << endl;
}

uint32_t TpccGenerator::makeAlphaString(uint32_t min, uint32_t max, char *dest)
{
   const static char *possible_values = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

   uint32_t len = makeNumber(min, max);
   for (uint32_t i = 0; i<len; i++) {
      dest[i] = possible_values[ranny() % 62];
   }
   if (len<max) {
      dest[len] = '\0';
   }
   return len;
}

uint32_t TpccGenerator::makeNumberString(uint32_t min, uint32_t max, char *dest)
{
   const static char *possible_values = "0123456789";

   uint32_t len = makeNumber(min, max);
   for (uint32_t i = 0; i<len; i++) {
      dest[i] = possible_values[ranny() % 10];
   }
   if (len<max) {
      dest[len] = '\0';
   }
   return len;
}

void TpccGenerator::makeAddress(char *street1, char *street2, char *city, char *state, char *zip)
{
   makeAlphaString(10, 20, street1);
   makeAlphaString(10, 20, street2);
   makeAlphaString(10, 20, city);
   makeAlphaString(2, 2, state);
   makeNumberString(9, 9, zip); // XXX
}

uint32_t TpccGenerator::makeNumber(uint32_t min, uint32_t max)
{
   return ranny() % (max - min + 1) + min;
}

uint32_t TpccGenerator::makeNonUniformRandom(uint32_t A, uint32_t x, uint32_t y)
{
   return ((makeNumber(0, A) | makeNumber(x, y)) + 42) % (y - x + 1) + x; // XXX
}

vector<uint32_t> TpccGenerator::makePermutation(uint32_t min, uint32_t max)
{
   assert(max>min);
   const uint32_t count = max - min;
   vector<uint32_t> result(count);
   iota(result.begin(), result.end(), min);

   for (uint32_t i = 0; i<count; i++) {
      swap(result[i], result[ranny() % count]);
   }
   return result;
}

void TpccGenerator::makeLastName(int64_t num, char *name)
{
   static const char *n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   strcpy(name, n[num / 100]);
   strcat(name, n[(num / 10) % 10]);
   strcat(name, n[num % 10]);
}

void TpccGenerator::makeNow(char *str)
{
   string s = to_string(chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count()); // XXX
   strncpy(str, s.data(), s.size());
}
