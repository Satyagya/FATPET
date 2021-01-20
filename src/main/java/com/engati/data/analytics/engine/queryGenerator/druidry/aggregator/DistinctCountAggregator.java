package com.engati.data.analytics.engine.queryGenerator.druidry.aggregator;

import in.zapr.druid.druidry.aggregator.DruidAggregator;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class DistinctCountAggregator extends DruidAggregator {

  private static final String DISTINCT_COUNT_TYPE_AGGREGATOR = "distinctCount";
  private String fieldName;

  public DistinctCountAggregator(@NonNull String name, @NonNull String fieldName) {
    this.type = DISTINCT_COUNT_TYPE_AGGREGATOR;
    this.name = name;
    this.fieldName = fieldName;
  }
}
