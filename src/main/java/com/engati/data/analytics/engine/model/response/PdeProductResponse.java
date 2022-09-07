package com.engati.data.analytics.engine.model.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Builder;
import lombok.Data;


@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class PdeProductResponse {

  private Long PRODUCT_productId;
  private String PRODUCT_title;
  private String IMAGE_url;
}