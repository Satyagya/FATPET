package com.engati.data.analytics.engine.model.response;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class DashboardGraphResponse {

  private String date;
  private Long queriesAsked;
  private Long queriesUnanswered;

}
