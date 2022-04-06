package com.engati.data.analytics.engine.model.response;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class OrderDetailsResponse {

  private Long orderId;
  private Long productId;
  private Long variantId;
  private Long collectionId;
  private Long customerId;
  private String createdDate;
  private Long botRef;
  private Double price;

}
