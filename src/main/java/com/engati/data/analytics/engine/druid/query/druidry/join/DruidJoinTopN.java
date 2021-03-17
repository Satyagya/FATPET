package com.engati.data.analytics.engine.druid.query.druidry.join;

import com.fasterxml.jackson.annotation.JsonInclude;
import in.zapr.druid.druidry.Context;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.dimension.DruidDimension;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.granularity.Granularity;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.QueryType;
import in.zapr.druid.druidry.topNMetric.TopNMetric;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class DruidJoinTopN {

  private QueryType queryType;
  private DruidJoin dataSource;
  private List<Interval> intervals;
  private Granularity granularity;
  private DruidFilter filter;
  private List<DruidAggregator> aggregations;
  private List<DruidPostAggregator> postAggregations;
  private Context context;
  private DruidDimension dimension;
  private int threshold;
  private TopNMetric metric;

  @Builder
  private DruidJoinTopN(@NonNull DruidJoin dataSource, @NonNull List<Interval> intervals,
      @NonNull Granularity granularity, DruidFilter filter,
      List<DruidAggregator> aggregators,
      List<DruidPostAggregator> postAggregators,
      @NonNull DruidDimension dimension, int threshold,
      @NonNull TopNMetric topNMetric, Context context) {

    this.queryType = QueryType.TOPN;
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.granularity = granularity;
    this.filter = filter;
    this.aggregations = aggregators;
    this.postAggregations = postAggregators;
    this.dimension = dimension;
    this.threshold = threshold;
    this.metric = topNMetric;
    this.context = context;
  }
}
