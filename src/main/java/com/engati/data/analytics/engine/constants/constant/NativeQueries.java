package com.engati.data.analytics.engine.constants.constant;

public class NativeQueries {

   public static String RECENCY_QUERY = "select customer_id from " +
       "(select customer_id, aggregator(created_date)as col_name " +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "where cancelled_at like 'None' " +
       "group by customer_id)as a " +
       "where col_name operator CURRENT_DATE - INTERVAL gap day;";

// atleast one order in past 12 month -> Greater than equal 1 order in last 365 days -> opeartor => GTE, gap => 365, orders_configured => 1
   public static String FREQUENCY_QUERY = "select customer_id from\n" +
       "(select customer_id, count(distinct order_id)as orders_in_last_gap_days\n" +
       "from\n" +
       "(select customer_id, order_id, created_date\n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL gap day)as a\n" +
       "group by customer_id)as b\n" +
       "where orders_in_last_gap_days operator orders_configured;";

   public static String MONETARY_QUERY = "select customer_id from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV,number_of_orders from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as float) total_price\n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c\n" +
       "where metric operator value";

   public static String STORE_AOV_QUERY = "select coalesce(round(sum_total*1.0/number_of_orders, 2), 0)as STORE_AOV from\n" +
       "(select sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select order_id, cast(total_price as float) total_price from\n" +
       " parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by order_id, total_price)as a)as b";

   public static String CUSTOMER_AOV_QUERY = "select customer_id, coalesce(round(AOV,2),0)as AOV from \n" +
       "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV from\n" +
       "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
       "(select customer_id, order_id, cast(total_price as float) total_price\n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "where cancelled_at like 'None'\n" +
       "and customer_id in customerSet" +
       "and created_date >= CURRENT_DATE - INTERVAL 12 MONTH\n" +
       "group by customer_id, order_id, total_price)as a\n" +
       "group by customer_id)as b)as c" ;

  public static String PRODUCT_TYPE_QUERY = "select distinct(product_type)\n" +
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/shopify_products_*.parquet') ";

   public static String ORDERS_FOR_X_MONTHS = "select customer_id, count(distinct order_id)as orders__last_gap_months \n" +
       "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "       where cancelled_at like 'None'\n" +
       "       and created_date > CURRENT_DATE - INTERVAL gap MONTH\n" +
       "       and customer_id in customerSet" +
       "       group by customer_id";

   public static String PRODUCT_VARIANTS_BY_UNIT_SALES = "with base as\n" +
       "       (select product_id, variant_id, count(variant_id) as variant_sales\n" +
       "          from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "             where product_id in\n" +
       "             (select product_id from\n" +
       "                     (select product_id, count(distinct order_id)as product_sales\n" +
       "                            from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') " +
       "                                   where cancelled_at like 'None'\n" +
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
       "          from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') where product_id in\n" +
       "             (select product_id from\n" +
       "                     (select product_id, count(distinct order_id)as product_sales\n" +
       "                            from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
       "                                                               where cancelled_at like 'None'\n" +
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
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') \n" +
      "where cancelled_at like 'None'\n" +
      "and created_at between from_date and to_date\n" +
      "and collection in (collection_name) \n" +
      "and product_type in (productType)";

  public static final String ORDERS_FOR_X_MONTH_WITH_FILTERS = "select customer_id from\n" +
      "(select customer_id, count(distinct order_id)as orders__last_gap_months\n" +
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet') \n" +
      "where cancelled_at like 'None'\n" +
      "and created_date > CURRENT_DATE - INTERVAL gap MONTH\n" +
      "group by customer_id)as a\n" +
      "where orders__last_gap_months operator value\n" ;

  public static final String CUSTOMER_AOV_QUERY_WITH_FILTERS = "select customer_id from\n" +
      "(select customer_id, coalesce(round(AOV, 2), 0)as AOV from\n" +
      "(select customer_id, ((sum_total)*1.0/(number_of_orders))as AOV from\n" +
      "(select customer_id, sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n" +
      "(select customer_id, order_id, cast(total_price as float) total_price\n" +
      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
      "where cancelled_at like 'None'\n" +
      "and created_date >= CURRENT_DATE - INTERVAL gap MONTH\n" +
      "group by customer_id, order_id, total_price)as a\n" +
      "group by customer_id)as b)as c)\n" +
      "where AOV operator value \n";

  public static final String CUSTOMER_AOV_QUERY_CUSTOM_SEGMENT = "select customer_id from\n" +
            "(select customer_id,coalesce(round(AOV,2),0) as AOV from\n" +
            "(select customer_id,((sum_total)*1.0/(number_of_orders)) as AOV from\n" +
            "(select customer_id,sum(total_price)as sum_total,count(distinct order_id)as number_of_orders from\n" +
            "(select customer_id,order_id,cast(total_price as float)as total_price\n" +
            "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
            "where cancelled_at like 'None'\n" +
            "and created_date between '_startdate_' and '_enddate_'\n" +
            "group by customer_id,order_id,total_price)as a\n" +
            "group by customer_id)as b)as c)\n" +
            "where AOV operator value\n";

  public static final String NUMBER_OF_ORDERS_CUSTOM_SEGMENT = "select customer_id from\n" +
            "(select customer_id,count(distinct  order_id)as total_orders from\n" +
            "(select customer_id, order_id, created_date\n" +
            "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
            "where cancelled_at like 'None'\n" +
            "and created_date between '_startdate_' and '_enddate_')as a\n" +
            "group by customer_id)as b where total_orders operator value\n";

  public static final String LAST_ORDER_DAYS_CUSTOM_SEGMENT = "select distinct(customer_id) from\n" +
            "parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
            "where cancelled_at like 'None'\n" +
            "and (created_date between '_startdate_' and '_enddate_') " +
            "and created_date>=CURRENT_DATE - INTERVAL gap day";

  public static final String SPEND_CUSTOM_SEGMENT = "select customer_id from\n" +
            "(select customer_id,sum(total_price)as revenue from\n" +
            "(select customer_id,order_id,cast(total_price as float)as total_price\n" +
            "from parquet_scan('"+ Constants.PARQUET_FILE_PATH + "/botRef/orders_*.parquet')\n" +
            "where cancelled_at like 'None'\n" +
            "and created_date between '_startdate_' and '_enddate_'\n" +
            "group by customer_id,order_id,total_price)as a\n" +
            "group by customer_id)as b where revenue operator value";

  public static final String GET_CUSTOMERS_FOR_PRODUCT_TYPE = "select distinct(customer_id) from\n" +
          "parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
          "where product_id in\n" +
          "(select product_id from\n" +
          "parquet_scan('"+ Constants.PARQUET_FILE_PATH + "/botRef/shopify_products_*.parquet')\n" +
          "where product_type in ProductTypes)\n" +
          "and cancelled_at like 'None'\n" +
          "and created_date between '_startdate_' and '_enddate_'";

  public static final String CUSTOMER_ORDERS = "select customer_id, count(distinct order_id)as total_orders from\n" +
            "parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')" +
            "where cancelled_at like 'None' \n" +
            "and created_date between '_startdate_' and '_enddate_'" +
            "and customer_id in customerSet\n" +
            "group by customer_id";

  public static final String CUSTOMER_AOV_CUSTOM_SEGMENT = "select customer_id,coalesce(round(AOV,2),0) as AOV from\n" +
                      "(select customer_id,((sum_total)*1.0/(number_of_orders)) as AOV from\n" +
                      "(select customer_id,sum(total_price)as sum_total,count(distinct order_id)as number_of_orders from\n" +
                      "(select customer_id,order_id,cast(total_price as float)as total_price\n" +
                      "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n" +
                      "where cancelled_at like 'None' \n" +
                      "and created_date between '_startdate_' and '_enddate_'" +
                      "and customer_id in customerSet\n" +
                      "group by customer_id,order_id,total_price)as a\n" +
                      "group by customer_id)as b)";

  public static final String CUSTOMER_SPEND = "select customer_id,round(revenue,2) as spend from\n" +
          "(select customer_id,sum(total_price)as revenue from\n" +
          "(select customer_id,order_id,cast(total_price as float)as total_price\n" +
          "from parquet_scan('"+ Constants.PARQUET_FILE_PATH + "/botRef/orders_*.parquet')\n" +
          "where cancelled_at like 'None'\n" +
          "and created_date between '_startdate_' and '_enddate_'\n" +
          "and customer_id in customerSet\n" +
          "group by customer_id,order_id,total_price)as a\n" +
          "group by customer_id)";

  public static final String CUSTOMER_LAST_ORDER_DATE = "select customer_id,max(created_date)as LAST_ORDER_DATE from\n" +
          "parquet_scan('"+ Constants.PARQUET_FILE_PATH + "/botRef/orders_*parquet')\n" +
          "where cancelled_at like 'None'\n" +
          "and (created_date between '_startdate_' and '_enddate_')\n" +
          "and customer_id in customerSet\n" +
          "group by customer_id";

  public static final String CUSTOMER_PRODUCT_TYPE = "select customer_id,GROUP_CONCAT(distinct(product_type), ',') as ProductTypes from\n" +
          "(select customer_id,product_id from\n" +
          "parquet_scan('"+ Constants.PARQUET_FILE_PATH + "/botRef/orders_*parquet')\n" +
          "where cancelled_at like 'None'\n" +
          "and created_date between '_startdate_' and '_enddate_'\n" +
          "and customer_id in customerSet\n" +
          "group by customer_id,product_id) as orders\n" +
          "natural join (select product_type,product_id from\n" +
          "parquet_scan('"+ Constants.PARQUET_FILE_PATH + "/botRef/shopify_products_*.parquet')) as products\n" +
          "group by customer_id";
  public static final String GET_ENGAGED_USERS = "select coalesce(count(distinct  user_id), 0)as users\n"
      + "from  parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/users_*.parquet')\n "
      + "where created_date between date '_date_' - interval 'gap' day and date '_date_'";

  public static final String GET_ORDER_COUNTS = "select coalesce(count(distinct  order_id), 0)as orders\n"
      + "from  parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n "
      + "where created_date between date '_date_' - interval 'gap' day and date '_date_'"
      + "and cancelled_at like 'None'";

  public static final String GET_AOV = "select coalesce(round(sum_total*1.0/number_of_orders, 2), 0)as AOV from\n"
      + "(select sum(total_price)as sum_total, count(distinct order_id)as number_of_orders from\n"
      + "(select order_id, cast(total_price as float) total_price from\n"
      + "parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n"
      + "where cancelled_at like 'None'\n"
      + "and created_date between date '_date_' - interval 'gap' day and date '_date_'\n"
      + "group by order_id, total_price)as a)as b";

  public static final String MOST_PURCHASED_PRODUCTS = "select product_id\n"
      + "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/orders_*.parquet')\n"
      + "where cancelled_at like 'None'\n"
      + "and created_date between date '_startdate_' and date '_enddate_'\n"
      + "group by product_id\n"
      + "order by count(product_id) desc\n"
      + "limit 3;";

  public static final String BOT_QUERIES_COUNTS = "select created_date, sum(queries_asked_count)as queries_asked,"
      + " sum(queries_unanswered_count)as queries_unanswered "
      + " from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/interactions_*.parquet')"
      + " where created_date between date '_startdate_' and date '_enddate_'\n"
      + " group by created_date"
      + " order by created_date";

  public static final String BOT_QUERIES_COUNTS_AGGREGATED = "select strftime(date_trunc('week', created_date), '%Y-%m-%d')as created_date, "
      + " ceil(avg(queries_asked_count))as queries_asked,"
      + " ceil(avg(queries_unanswered_count))as queries_unanswered "
      + " from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/interactions_*.parquet')"
      + " where created_date between date '_startdate_' and date '_enddate_'\n"
      + " group by created_date"
      + " order by created_date";

  public static final String GET_ENGAGED_USERS_BY_PLATFORM = "select count(distinct  user_id)as users, platform\n"
      + "from  parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/users_*.parquet')\n "
      + "where created_date between date '_startdate_' and date '_enddate_'\n"
      + "group by platform ";

  public static final String GET_CONVERSATION_INTENT = "select sum(intent_count)as intent_count_sum, intent_label\n"
      + "from  parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/intents_*.parquet')\n "
      + "where created_date between date '_startdate_' and date '_enddate_'\n"
      + "and intent_label not like 'None' and intent_label in ('ORDER_ENQUIRY', 'PURCHASE_ENQUIRY', "
      + "'RETURN_AND_EXCHANGE', 'PRICE_ENQUIRY', 'OUT_OF_STOCK_ENQUIRY')\n"
      + "group by intent_label ";

  public static final String GET_CONVERSATION_SENTIMENT = "select sum(sentiment_count)as sentiment_count_sum, "
    + "sentiment_label\n"
      + "from  parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/sentiments_*.parquet')\n "
      + "where created_date between date '_startdate_' and date '_enddate_'\n"
      + "and sentiment_label not like 'None'"
      + "group by sentiment_label ";


    public static final String GET_TRANSACTIONS_FROM_ENGATI = "select coalesce(count(distinct transactionId), 0) as transactions "
            + "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/ga_transactions_*.parquet') \n "
            + "where source like '%engati%' \n";

  public static final String GET_TRANSACTION_REVENUE_FROM_ENGATI = "select round(sum(coalesce(transactionRevenue, 0)),2)as transaction_revenue "
          + "from parquet_scan('"+ Constants.PARQUET_FILE_PATH +"/botRef/ga_transactions_*.parquet') \n "
          + "where source like '%engati%' \n"
      + "and created_date between date '_date_' - interval 'gap' day and date '_date_'";

  public static final String MOST_ABANDONED_PRODUCTS =
      "SELECT product_id FROM "
          + "PARQUET_SCAN('%s') WHERE created_date BETWEEN DATE '%s' AND DATE '%s' GROUP BY "
          + "product_id ORDER BY COUNT(product_id) DESC LIMIT 3";

  public static final String ABANDONED_CHECKOUTS_BY_TIME_RANGE =
      "SELECT COUNT(checkout_id) as abandoned_checkouts FROM PARQUET_SCAN('%s') WHERE created_date BETWEEN DATE '%s' - "
          + "INTERVAL %d DAY AND '%s'";
}
