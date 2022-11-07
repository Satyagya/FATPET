package com.engati.data.analytics.engine.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Data
@Entity
@Table(name = "shopify_product_discovery_config")
public class ShopifyProductDiscoveryConfig {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Column(name = "store_domain")
  private String storeDomain;

  @Column(name = "bot_ref")
  private Integer botRef;

  @Column(name = "customer_id")
  private Integer customerId;

  @Column(name = "product_discovery_enabled")
  private Integer productDiscoveryEnabled;

  @Column(name = "categorisation_type")
  private String categorisationType;

  @Column(name = "sorting_order")
  private String sortingOrder;

  @Column(name = "show_out_of_stock")
  private Integer showOutOfStock;

  @Column(name = "categorisation_list")
  private String categorisationList;

  @Column(name = "created_at")
  private Timestamp createdAt;

  @Column(name = "updated_at")
  private Timestamp updatedAt;

  @Column(name = "is_tag_generation_enabled")
  private Timestamp isTagGenerationEnabled;

  @Column(name = "is_similar_products_enabled")
  private Timestamp isSimilarProductsEnabled;

}
