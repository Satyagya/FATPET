package com.engati.data.analytics.engine.model.response;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class DashboardFlierResponse {

  private Double presentValue;
  private Double percentageChange;
  private String currency;

}
