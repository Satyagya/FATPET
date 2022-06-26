package com.engati.data.analytics.engine.model.response;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class PdeProductResponse {

  private Long PRODUCT_productId;
  private String PRODUCT_title;
  private String IMAGE_url;
}