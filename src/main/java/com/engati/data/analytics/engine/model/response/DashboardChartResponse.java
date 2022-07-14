package com.engati.data.analytics.engine.model.response;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class DashboardChartResponse {

  private Map<String, Double> metricPercentage;

}