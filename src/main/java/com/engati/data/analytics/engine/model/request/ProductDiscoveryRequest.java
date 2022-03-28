package com.engati.data.analytics.engine.model.request;

import lombok.Data;

import java.util.List;

@Data
public class ProductDiscoveryRequest {

  private String collection;
  private String productType;
  private String productTag;
  private Integer numberOfVariantsInResponse;
  private List<Long> productIds;

}
