package com.engati.data.analytics.engine.entity;

import lombok.*;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.sql.Timestamp;

@Data
@Entity
@Table(name = "shopify_customer")
public class ShopifyCustomer {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Column(name = "user_id")
  private String userId;

  @Column(name = "bot_ref")
  private Integer botRef;

  @Column(name = "shop_domain")
  private String shopDomain;

  @Column(name = "customer_email")
  private String customerEmail;

  @Column(name = "customer_phone")
  private String customerPhone;

  @Column(name = "created_at")
  private Timestamp createdAt;

  @Column(name = "updated_at")
  private Timestamp updatedAt;

  @Column(name = "first_name")
  private String firstName;

  @Column(name = "last_name")
  private String lastName;

  @Column(name = "orders_count")
  private Integer ordersCount;

  @Column(name = "total_spent")
  private Double totalSpent;

  @Column(name = "last_order_id")
  private Long lastOrderId;

  @Column(name = "last_order_name")
  private String lastOrderName;

  @Column(name = "default_address_city")
  private String defaultAddressCity;

  @Column(name = "default_address_province")
  private String defaultAddressProvince;

  @Column(name = "default_address_country")
  private String defaultAddressCountry;

}

