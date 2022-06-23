package com.engati.data.analytics.engine.model.response;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class DashboardProductResponse {

  private Long productId;
  private String productTitle;
  private String productImage;

}
