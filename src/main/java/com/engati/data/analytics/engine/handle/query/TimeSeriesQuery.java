package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.util.Uitility;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.GroupByQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TimeSeriesQueryMetaInfo;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.aggregation.DruidTimeSeriesQuery;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public class TimeSeriesQuery extends QueryHandler {

  private static final String QUERY_TYPE_TIMESERIES = "timeseries";

  @Autowired
  private DruidQueryGenerator druidQueryGenerator;

  @Override
  public String getQueryType() {
    return QUERY_TYPE_TIMESERIES;
  }

  @Override
  public List<List<Map<String, String>>> generateAndExecuteQuery(Integer botRef, Integer
      customerId, DruidQueryMetaInfo druidQueryMetaInfo) {
    TimeSeriesQueryMetaInfo timeSeriesQueryMetaInfo = ((TimeSeriesQueryMetaInfo)
        druidQueryMetaInfo);

    Interval interval = null;

    List<DruidAggregator> druidAggregators = druidQueryGenerator
        .generateAggregators(timeSeriesQueryMetaInfo.getDruidAggregateMetaInfo());

    List<DruidPostAggregator> postAggregators = druidQueryGenerator
        .generatePostAggregator(timeSeriesQueryMetaInfo.getDruidPostAggregateMetaInfo());
    DruidFilter druidFilter = druidQueryGenerator
        .generateFilters(timeSeriesQueryMetaInfo.getDruidFilterMetaInfo().get(0));

    DruidTimeSeriesQuery timeSeriesQuery = DruidTimeSeriesQuery.builder()
        .dataSource(Uitility.convertDataSource(botRef, customerId,
            timeSeriesQueryMetaInfo.getDataSource()))
        .intervals(Collections.singletonList(interval))
        .aggregators(druidAggregators)
        .postAggregators(postAggregators)
        .filter(druidFilter)
        .build();

    Uitility.convertDruidQueryToJsonString(timeSeriesQuery);
    return null;
  }
}
