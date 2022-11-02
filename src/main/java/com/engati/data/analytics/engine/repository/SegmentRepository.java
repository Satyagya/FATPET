package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.ShopifyCustomer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Repository("com.engati.broadcast.repository.SegmentsRepository")
public interface SegmentRepository extends JpaRepository<ShopifyCustomer, Long> {

  @Query(value = "select customer_id, coalesce(customer_email, '')as customer_email, customer_phone,\n" +
      "       concat(COALESCE(first_name, \"\") ,' ', COALESCE(last_name, \"\"))as customer_name " +
      "from shopify_customer " +
      "where customer_id in :customerIds ", nativeQuery = true)
  List<Map<Long, Object>> findByShopifyCustomerId(@Param("customerIds") Set<Long> customerIds);

  @Query(value = "select shop_domain from shopify_customer where bot_ref = :botRef ",
         nativeQuery = true)
  String findShopDomainByBotRef(@Param("botRef") Long botRef);
}
