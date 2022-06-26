package com.engati.data.analytics.engine.constants.constant;

public class NativeQueries {

   public static String RECENCY_QUERY = "select customer_id from " +
       "(select customer_id, aggregator(created_date)as col_name " +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like 'None' " +
       "and (is_test like 'nan' or is_test = 0) " +
       "group by customer_id)as a " +
       "where col_name operator CURRENT_DATE - INTERVAL gap day;";

// atleast one order in past 12 month -> Greater than equal 1 order in last 365 days -> opeartor => GTE, gap => 365, orders_configured => 1
   public static String FREQUENCY_QUERY = "select customer_id from\n" +
       "(select customer_id, count(distinct order_id)as orders_in_last_gap_days\n" +
       "from\n" +
       "(select customer_id, order_id, created_date\n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and (is_test like 'nan' or is_test = 0) " +
       "and created_date >= CURRENT_DATE - INTERVAL gap day)as a\n" +
       "group by customer_id)as b\n" +
       "where orders_in_last_gap_days operator orders_configured;";

   public static String MONETARY_QUERY = "select customer_id from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV,number_of_orders from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as float) total_price\n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and (is_test like 'nan' or is_test = 0) " +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c\n" +
       "where metric operator value";

   public static String STORE_AOV_QUERY = "select round(sum_total*1.0/number_of_orders, 2)as STORE_AOV from\n" +
       "(select sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select order_id, cast(total_price as float) total_price from\n" +
       " parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and (is_test like 'nan' or is_test = 0) " +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by order_id, total_price)as a)as b";

   public static String CUSTOMER_AOV_QUERY = "select customer_id, round(AOV,2)as AOV from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as float) total_price\n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and (is_test like 'nan' or is_test = 0) " +
       "and customer_id in customerSet" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c" ;

   public static String ORDERS_FOR_X_MONTHS = "select customer_id, count(distinct order_id)as orders__last_gap_months \n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "       where cancelled_at like 'None'\n" +
       "       and (is_test like 'nan' or is_test = 0) " +
       "       and created_date >= CURRENT_DATE - INTERVAL gap MONTH\n" +
       "       and customer_id in customerSet" +
       "       group by customer_id";

   public static String PRODUCT_VARIANTS_BY_UNIT_SALES = "with base as\n" +
       "       (select product_id, variant_id, count(variant_id) as variant_sales\n" +
       "          from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "             where product_id in\n" +
       "             (select product_id from\n" +
       "                     (select product_id, count(distinct order_id)as product_sales\n" +
       "                            from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') " +
       "                                   where cancelled_at like 'None'\n" +
       "                                   and (is_test like 'nan' or is_test = 0) " +
       "                                   --add product_type-- \n" +
       "                                   --add collection-- \n" +
       "                                   --add product_tags-- \n" +
       "                                   --add productIds-- \n" +
       "                                   group by product_id\n" +
       "                     )\n" +
       "              )\n" +
       "              group by product_id, variant_id\n" +
       "              order by variant_sales\n" +
       "              DESC\n" +
       "       )\n" +
       "select product_id, variant_id\n" +
       "from base \n" +
       "QUALIFY \n" +
       "row_number() over (partition by product_id) <= 2\n" +
       "order by variant_sales desc\n" +
       "limit number_of_results";

   public static String PRODUCT_VARIANTS_BY_UNIT_SALES_TEST = "with base as\n" +
       "         (select product_id, variant_id, count(variant_id)as variant_sales\n" +
       "          from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') where product_id in\n" +
       "             (select product_id from\n" +
       "                     (select product_id, count(distinct order_id)as product_sales\n" +
       "                            from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet')\n" +
       "                                                               where cancelled_at like 'None'\n" +
       "                                                               and (is_test like 'nan' or is_test = 0) " +
       "                                   group by product_id\n" +
       "                     )as a \n" +
       "              ) \n" +
       "group by product_id, variant_id\n" +
       "    )\n" +
       "select * from(\n" +
       "select product_id, variant_id,\n" +
       "       row_number() over (partition by product_id order by variant_sales desc)as row_num\n" +
       "from base\n" +
       "    order by variant_sales desc)as foo\n" +
       "where row_num <= 2\n" +
       "limit number_of_results";

  public static final String PURCHASE_HISTORY = "select order_id, line_item_id, product_id, variant_id, collection_id, customer_id, created_at, bot_ref, line_item_price\n" +
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') \n" +
      "where cancelled_at like 'None'\n" +
      "and (is_test like 'nan' or is_test = 0) " +
      "and created_at between from_date and to_date\n" +
      "and collection in (collection_name) \n" +
      "and product_type in (productType)";

  public static final String ORDERS_FOR_X_MONTH_WITH_FILTERS = "select customer_id from\n" +
      "(select customer_id, count(distinct order_id)as orders__last_gap_months\n" +
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet') \n" +
      "where cancelled_at like 'None'\n" +
      "and (is_test like 'nan' or is_test = 0) " +
      "and created_date >= CURRENT_DATE - INTERVAL gap MONTH\n" +
      "group by customer_id)as a\n" +
      "where orders__last_gap_months operator value\n" ;

  public static final String CUSTOMER_AOV_QUERY_WITH_FILTERS = "select customer_id from\n" +
      "(select customer_id, round(AOV, 2)as AOV from\n" +
      "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV from\n" +
      "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
      "(select customer_id, order_id, cast(total_price as float) total_price\n" +
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/*.parquet')\n" +
      "where cancelled_at like 'None'\n" +
      "and (is_test like 'nan' or is_test = 0) " +
      "and created_date >= CURRENT_DATE - INTERVAL gap MONTH\n" +
      "group by customer_id, order_id, total_price)as a\n" +
      "group by customer_id)as b)as c)\n" +
      "where AOV operator value \n";

}
