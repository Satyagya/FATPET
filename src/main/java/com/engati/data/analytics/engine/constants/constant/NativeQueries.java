package com.engati.data.analytics.engine.constants.constant;

public class NativeQueries {

   public static String RECENCY_QUERY = "select customer_id from " +
       "(select customer_id, aggregator(created_date)as col_name " +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like '%nan%' " +
       "group by customer_id)as a " +
       "where col_name operator CURRENT_DATE - INTERVAL gap day;";

// atleast one order in past 12 month -> Greater than equal 1 order in last 365 days -> opeartor => GTE, gap => 365, orders_configured => 1
   public static String FREQUENCY_QUERY = "select customer_id from\n" +
       "(select customer_id, count(distinct order_id)as orders_in_last_gap_days\n" +
       "from\n" +
       "(select customer_id, order_id, created_date\n" +
    "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like '%nan%'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL gap day)as a\n" +
       "group by customer_id)as b\n" +
       "where orders_in_last_gap_days operator orders_configured;";

   public static String MONETARY_QUERY = "select customer_id from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV,number_of_orders from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as int) total_price\n" +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like '%nan%'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c\n" +
       "where metric operator value";

   public static String STORE_AOV_QUERY = "select round(sum_total*1.0/number_of_orders, 2)as STORE_AOV from\n" +
       "(select sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select order_id, cast(total_price as int) total_price from\n" +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like '%nan%'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by order_id, total_price)as a)as b";

   public static String CUSTOMER_AOV_QUERY = "select customer_id, round(AOV,2)as AOV from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as int) total_price\n" +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like '%nan%'\n" +
       "and customer_id in customerSet" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c" ;

   public static String ORDERS_FOR_X_MONTHS = "select customer_id, count(distinct order_id)as orders__last_gap_months \n" +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "       where cancelled_at like '%nan%'\n" +
       "       and created_date >= CURRENT_DATE - INTERVAL gap MONTH\n" +
       "       and customer_id in customerSet" +
       "       group by customer_id";

   public static String PRODUCT_VARIANTS_BY_UNIT_SALES = "select product_id, variant_id\n" +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where product_id in\n" +
       "(select product_id from \n" +
       "(select product_id, count(distinct order_id)as OrderCount\n" +
       "from parquet_scan('"+ Constants.PAQUET_FILE_PATH +"/botRef/*.parquet') " +
       "where cancelled_at like '%nan%'\n" +
       "--add product_type-- \n" +
       "--add collection-- \n" +
       "--add product_tags-- \n" +
       "--add productIds-- \n" +
       "group by product_id))\n" +
       "group by product_id, variant_id\n" +
       "order by count(variant_id) DESC\n" +
       "Limit number_of_results";

}
