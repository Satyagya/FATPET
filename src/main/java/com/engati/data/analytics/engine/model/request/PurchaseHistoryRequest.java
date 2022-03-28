package com.engati.data.analytics.engine.model.request;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class PurchaseHistoryRequest {

  private Long botRef;
  private Long customerId;
  private Timestamp startTime;
  private Timestamp endTime;
  private String collection;
  private String productType;

}
