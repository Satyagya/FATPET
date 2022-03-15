package com.engati.data.analytics.engine.constants;

public class QueryConstants {

   public static String recencyQuery = "select customer_id from " +
       "(select customer_id, aggregator(created_date)as col_name " +
       "from parquet_scan('parquet_store/botRef/*.parquet') " +
       "where cancelled_at like '%nan%' " +
       "group by customer_id)as a " +
       "where col_name operator CURRENT_DATE - INTERVAL gap day;";

   public static String frequencyQuery = "select customer_id from\n" +
       "(select customer_id, count(distinct order_id)as orders_in_last_gap_days\n" +
       "from\n" +
       "(select customer_id, order_id, created_date\n" +
       "from parquet_scan('parquet_store/botRef/*.parquet')\n" +
       "where cancelled_at like '%nan%'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL gap day)as a\n" +
       "group by customer_id)as b\n" +
       "where orders_in_last_gap_days operator orders_configured;";
// operator used to handle scenarios around -> more than 20 orders in 30 days, let than 3 orders in 90 days

   public static String monetaryQuery = "select customer_id from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV,number_of_orders from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as int) total_price\n" +
       "from parquet_scan('parquet_store/botRef/*.parquet')\n" +
       "where cancelled_at like '%nan%'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL 6 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c\n" +
       "where metric operator value";

   public static String storeAOVQuery = "select round(sum_total*1.0/number_of_orders, 2)as Store_AOV from\n" +
       "(select sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select order_id, cast(total_price as int) total_price from\n" +
       "parquet_scan('parquet_store/botRef/*.parquet')\n" +
       "where cancelled_at like '%nan%'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL 6 MONTH\n" +
       "group by order_id, total_price)as a)as b";

}
