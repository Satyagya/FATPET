package com.engati.data.analytics.engine.druid.query.druidry;

import com.fasterxml.jackson.annotation.JsonInclude;
import in.zapr.druid.druidry.Context;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.query.DruidQuery;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
public class ScanQuery {

  private static final String SCAN_QUERY = "scan";
  private String queryType;
  private String dataSource;
  private List<Interval> intervals;
  private List<String> columns;
  private DruidFilter filter;
  private Context context;

  @Builder
  private ScanQuery(@NonNull String dataSource, @NonNull List<Interval> intervals,
      List<String> columns, DruidFilter filter, Context context) {

    this.queryType = SCAN_QUERY;
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.columns = columns;
    this.filter = filter;
    this.context = context;
  }
}
