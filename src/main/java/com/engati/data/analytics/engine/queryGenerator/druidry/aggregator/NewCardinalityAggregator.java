package com.engati.data.analytics.engine.queryGenerator.druidry.aggregator;

import com.fasterxml.jackson.annotation.JsonInclude;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class NewCardinalityAggregator extends DruidAggregator {

  private static final String CARDINALITY_AGGREGATOR_TYPE = "cardinality";
  private List<String> fields;
  private Boolean byRow;
  private Boolean round;

  public NewCardinalityAggregator(@NonNull String name, @NonNull List<String> fields,
      Boolean byRow, Boolean round) {
    this.type = CARDINALITY_AGGREGATOR_TYPE;
    this.name = name;
    this.fields = fields;
    this.byRow = byRow;
    this.round = round;
  }
}
