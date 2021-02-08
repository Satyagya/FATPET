package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TimeSeriesQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.aggregation.DruidTimeSeriesQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class TimeSeriesQuery extends QueryHandler {

  private static final String QUERY_TYPE = "TIME_SERIES";

  @Autowired
  private DruidQueryGenerator druidQueryGenerator;

  @Autowired
  private DruidQueryExecutor druidQueryExecutor;

  @Autowired
  private DruidResponseParser druidResponseParser;

  @Override
  public String getQueryType() {
    return QUERY_TYPE;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer
      customerId, DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    TimeSeriesQueryMetaInfo timeSeriesQueryMetaInfo = ((TimeSeriesQueryMetaInfo)
        druidQueryMetaInfo);

    List<DruidAggregator> druidAggregators = druidQueryGenerator
        .generateAggregators(timeSeriesQueryMetaInfo.getDruidAggregateMetaInfo());
    List<DruidPostAggregator> postAggregators = druidQueryGenerator
        .generatePostAggregator(timeSeriesQueryMetaInfo.getDruidPostAggregateMetaInfo());
    DruidFilter druidFilter = druidQueryGenerator
        .generateFilters(timeSeriesQueryMetaInfo.getDruidFilterMetaInfo());

    DruidTimeSeriesQuery timeSeriesQuery = DruidTimeSeriesQuery.builder()
        .dataSource(Utility.convertDataSource(botRef, customerId,
            timeSeriesQueryMetaInfo.getDataSource()))
        .intervals(Utility.extractInterval(timeSeriesQueryMetaInfo.getIntervals()))
        .aggregators(druidAggregators)
        .postAggregators(postAggregators)
        .filter(druidFilter)
        .granularity(Utility.getGranularity(timeSeriesQueryMetaInfo.getGrain()))
        .build();

    String query = Utility.convertDruidQueryToJsonString(timeSeriesQuery);
    JsonArray response = druidQueryExecutor.getResponseFromDruid(query);
    SimpleResponse simpleResponse = SimpleResponse.builder()
        .queryResponse(druidResponseParser.convertJsonToMap(response)).build();
    simpleResponse.setType(ResponseType.SIMPLE);
    return simpleResponse;
  }
}
