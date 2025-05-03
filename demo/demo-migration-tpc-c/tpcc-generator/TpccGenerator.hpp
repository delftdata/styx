#pragma once

#include "CsvWriter.hpp"

#include <cstdint>
#include <string>
#include <random>
#include <unordered_set>

static constexpr uint32_t kItemCount = 100000;
static constexpr uint32_t kCustomerPerDistrict = 3000;
static constexpr uint32_t kDistrictsPerWarehouse = 10;
static constexpr uint32_t OrdersPerDistrict = 3000;
static constexpr uint32_t newOrdersPerDistrict = 900;
static constexpr uint32_t MONEY_DECIMALS = 2;
static constexpr uint32_t MIN_NAME = 6;
static constexpr uint32_t MAX_NAME = 10;
static constexpr uint32_t MIN_STREET = 10;
static constexpr uint32_t MAX_STREET = 20;
static constexpr uint32_t MIN_CITY = 10;
static constexpr uint32_t MAX_CITY = 20;
static constexpr uint32_t STATE = 2;
static constexpr uint32_t ZIP_LENGTH = 9;
const static std::string ZIP_SUFFIX = "11111";
static constexpr float INITIAL_W_YTD = 300000.00f;
static constexpr float MIN_TAX = 0.0000;
static constexpr float MAX_TAX = 0.2000;
static constexpr uint32_t TAX_DECIMALS = 4;
static constexpr uint32_t INITIAL_YTD_DECIMALS = 2;
static constexpr float INITIAL_D_YTD = 30000.00f;
static constexpr uint32_t MIN_FIRST = 6;
static constexpr uint32_t MAX_FIRST = 10;
static constexpr uint32_t MAX_LAST = 16;
static constexpr uint32_t PHONE = 16;
static constexpr uint32_t DATETIME_SIZE = 26;
static constexpr float INITIAL_CREDIT_LIM = 50000.00;
static constexpr float MIN_DISCOUNT = 0.0000;
static constexpr float MAX_DISCOUNT = 0.5000;
static constexpr uint32_t DISCOUNT_DECIMALS = 4;
static constexpr float INITIAL_BALANCE = -10.00f;
static constexpr uint32_t MIN_C_DATA = 300;
static constexpr uint32_t MAX_C_DATA = 500;
const static std::string C_MIDDLE = "OE";
static constexpr uint32_t INITIAL_DELIVERY_CNT = 0;
static constexpr uint32_t INITIAL_PAYMENT_CNT = 1;
static constexpr float INITIAL_YTD_PAYMENT = 10.00;
// HISTORY CONSTS
static constexpr float INITIAL_AMOUNT = 10.00;
static constexpr uint32_t MIN_DATA = 12;
static constexpr uint32_t MAX_DATA = 24;
// ITEM CONSTS
static constexpr uint32_t MIN_I_DATA = 26;
static constexpr uint32_t MAX_I_DATA = 50;
static constexpr uint32_t MIN_I_NAME = 14;
static constexpr uint32_t MAX_I_NAME = 24;
static constexpr float MIN_PRICE = 1.00;
static constexpr float MAX_PRICE = 100.00;
static constexpr uint32_t MIN_IM = 1;
static constexpr uint32_t MAX_IM = 10000;
// STOCK CONSTS
static constexpr uint32_t MIN_QUANTITY = 10;
static constexpr uint32_t MAX_QUANTITY = 100;
static constexpr uint32_t DIST = 24;
static constexpr uint32_t STOCK_PER_WAREHOUSE = 100000;
// ORDER CONSTS
static constexpr uint32_t MIN_CARRIER_ID = 1;
static constexpr uint32_t MAX_CARRIER_ID = 10;
static constexpr uint32_t MIN_OL_CNT = 5;
static constexpr uint32_t MAX_OL_CNT = 15;
//  ORDER-LINE CONSTS
static constexpr uint32_t INITIAL_QUANTITY = 5;
static constexpr float MIN_AMOUNT = 0.01;

class TpccGenerator {

   // If these are different the order generation needs to be changed.
   // Right now there is a 1:1 relationship between customers and orders.
   static_assert(kCustomerPerDistrict == OrdersPerDistrict, "These should match, see comment.");

   const uint32_t warehouse_count;
   const std::string folder;

   std::mt19937 ranny;

   uint32_t makeAlphaString(uint32_t min, uint32_t max, char *dest);
   uint32_t makeNumberString(uint32_t min, uint32_t max, char *dest);
   uint32_t makeNumber(uint32_t min, uint32_t max);
   uint32_t makeNumberExcluding(uint32_t minimum, uint32_t maximum, uint32_t excluding);
   float fixedPoint(uint32_t decimal_places, float min, float max);
   std::vector<uint32_t> makePermutation(uint32_t min, uint32_t max);
   void makeAddress(char *str1, char *street2, char *city, char *state, char *zip);
   static void makeLastName(int64_t num, char *name);
   void makeRandomLastName(char *name);
   uint32_t NURand(uint32_t a, uint32_t x, uint32_t y);
   static void makeNow(char *str);
   std::unordered_set<uint32_t> selectUniqueIds(uint32_t numUnique, uint32_t minimum, uint32_t maximum);
   void generateItem(csv::CsvWriter& i_csv, uint32_t i_id, std::unordered_set<uint32_t>& originalRows);
   void generateWarehouse(csv::CsvWriter& w_csv, uint32_t w_id);
   void generateDistrict(csv::CsvWriter& d_csv, uint32_t d_w_id, uint32_t d_id, uint32_t d_next_o_id);
   void generateCustomer(csv::CsvWriter& c_csv, uint32_t c_w_id, uint32_t c_d_id, uint32_t c_id, std::unordered_set<uint32_t>& bad_customers);
   void generateHistory(csv::CsvWriter& h_csv, uint32_t h_c_w_id, uint32_t h_c_d_id, uint32_t h_c_id);
   void generateOrder(csv::CsvWriter& o_csv, uint32_t o_w_id, uint32_t o_d_id, uint32_t o_id, uint32_t o_c_id, uint32_t o_ol_cnt, bool new_order);
   void generateOrderLine(csv::CsvWriter& ol_csv, uint32_t ol_w_id, uint32_t ol_d_id, uint32_t ol_o_id, uint32_t ol_number, uint32_t max_items, bool new_order);
   void generateStock(csv::CsvWriter& s_csv, uint32_t s_w_id, uint32_t s_i_id, std::unordered_set<uint32_t>& original_items);

public:
   TpccGenerator(uint32_t warehouse_count, std::string folder);
   void setRandomSeed(const uint32_t seed) { ranny.seed(seed); }
   void generateWarehouses();
   void generateItems();
};
