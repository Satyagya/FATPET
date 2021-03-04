package com.engati.data.analytics.engine.druid.query.druidry.join;

import com.fasterxml.jackson.annotation.JsonInclude;
import in.zapr.druid.druidry.Context;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.granularity.Granularity;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.QueryType;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class DruidJoinTimeSeries {

  private QueryType queryType;
  private DruidJoin dataSource;
  private Boolean descending;
  private List<Interval> intervals;
  private Granularity granularity;
  private DruidFilter filter;
  private List<DruidAggregator> aggregations;
  private List<DruidPostAggregator> postAggregations;
  private Context context;

  @Builder
  private DruidJoinTimeSeries(@NonNull DruidJoin dataSource, Boolean descending,
      @NonNull List<Interval> intervals, @NonNull Granularity granularity,
      DruidFilter filter, List<DruidAggregator> aggregators,
      List<DruidPostAggregator> postAggregators, Context context) {

    this.queryType = QueryType.TIMESERIES;
    this.dataSource = dataSource;
    this.descending = descending;
    this.intervals = intervals;
    this.granularity = granularity;
    this.filter = filter;
    this.aggregations = aggregators;
    this.postAggregations = postAggregators;
    this.context = context;
  }
}
