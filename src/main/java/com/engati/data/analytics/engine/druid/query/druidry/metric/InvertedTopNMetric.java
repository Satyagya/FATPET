package com.engati.data.analytics.engine.druid.query.druidry.metric;

import in.zapr.druid.druidry.topNMetric.TopNMetric;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class InvertedTopNMetric extends TopNMetric {
  private static final String INVERTED_METRIC = "inverted";
  private String type;
  private String metric;

  public InvertedTopNMetric(String metric) {
    this.type = INVERTED_METRIC;
    this.metric = metric;
  }
}
