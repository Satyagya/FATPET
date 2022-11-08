package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.ShopifyCustomer;
import com.engati.data.analytics.engine.entity.ShopifyProductDiscoveryConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository("com.engati.broadcast.repository.ShopifyProductDiscoveryConfigRepository")
public interface ShopifyProductDiscoveryConfigRepository extends JpaRepository<ShopifyProductDiscoveryConfig, Long> {
  @Query(value = "select store_domain from shopify_product_discovery_config where bot_ref = :botRef limit 1",
         nativeQuery = true)
  String findShopDomainByBotRef(@Param("botRef") Long botRef);
}
