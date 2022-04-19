package com.engati.data.analytics.engine.model.request;

import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
public class PurchaseHistoryRequest {

  private Long botRef;
  private Timestamp startTime;
  private Timestamp endTime;
  private List<String> collection;
  private List<String> productType;

}
