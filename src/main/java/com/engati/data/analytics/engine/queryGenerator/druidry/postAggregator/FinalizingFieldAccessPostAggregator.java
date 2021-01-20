package com.engati.data.analytics.engine.queryGenerator.druidry.postAggregator;

import com.fasterxml.jackson.annotation.JsonInclude;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import lombok.Getter;
import lombok.NonNull;

@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FinalizingFieldAccessPostAggregator extends DruidPostAggregator {
  private final static String FIELD_ACCESS_POST_AGGREGATOR_TYPE = "finalizingFieldAccess";

  private String fieldName;

  public FinalizingFieldAccessPostAggregator(@NonNull String name, @NonNull String fieldName) {
    this.type = FIELD_ACCESS_POST_AGGREGATOR_TYPE;
    this.name = name;
    this.fieldName = fieldName;
  }

  public FinalizingFieldAccessPostAggregator(@NonNull String fieldName) {
    this.type = FIELD_ACCESS_POST_AGGREGATOR_TYPE;
    this.fieldName = fieldName;
  }
}
