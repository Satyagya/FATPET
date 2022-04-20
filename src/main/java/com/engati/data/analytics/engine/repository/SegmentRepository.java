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

  @Query(value = "select id as customer_id, customer_email, customer_phone,\n" +
      "       concat(COALESCE(first_name, \"\") ,' ', COALESCE(last_name, \"\"))as customer_name\n" +
      "from shopify_customer\n" +
      "where id in :customerIds ", nativeQuery = true)
  List<Map<Long, Object>> findByShopifyCustomerId(@Param("customerIds") Set<Long> customerIds);
}
