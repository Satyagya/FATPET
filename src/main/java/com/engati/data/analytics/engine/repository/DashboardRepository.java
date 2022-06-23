package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.ShopifyCustomer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Repository("com.engati.broadcast.repository.DashboardRepository")
public interface DashboardRepository  extends JpaRepository<ShopifyCustomer, Long> {


  @Query(value ="select currency from shopify_orders where bot_ref = :botRef \n"
      + "order by count(*) desc limit 1", nativeQuery = true)
  String findCurrencyByBotRef(@Param("botRef") Long botRef);


  @Query(value = "select count(*)as checkouts from shopify_checkouts\n"
      + "where date(created_at) between :_date_ - interval :gap day and :_date_ \n"
      + "and bot_ref = :botRef "
      + "and checkout_id not in (select checkout_id from shopify_orders where checkout_id is not "
      + "null)", nativeQuery = true)
  Long getAbandonedCheckoutsbyBotRefbyTimeRange(@Param("botRef") Long botRef, @Param("_date_") String date_, @Param("gap") Long gap);


  @Query(value = "select variant_id from shopify_checkout_line_items\n"
      + "where bot_ref = :botRef "
      + "and checkout_id in (select checkout_id from shopify_checkouts\n"
      + "    where date(created_at) between :_startdate_ and :_enddate_ )\n"
      + "group by variant_id order by count(variant_id) desc limit 3\n", nativeQuery = true)
  List<Long> getMostAbandonedProductsByBotRef(@Param("botRef") Long botRef, @Param("_startdate_") String startdate, @Param("_enddate_") String enddate);
}
