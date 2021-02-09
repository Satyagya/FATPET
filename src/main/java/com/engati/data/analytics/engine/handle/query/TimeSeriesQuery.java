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
import java.util.Objects;

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
        .generateAggregators(timeSeriesQueryMetaInfo.getDruidAggregateMetaInfo(),
            botRef, customerId);
    List<DruidPostAggregator> postAggregators = druidQueryGenerator
        .generatePostAggregator(timeSeriesQueryMetaInfo.getDruidPostAggregateMetaInfo(),
            botRef, customerId);
    DruidFilter druidFilter = druidQueryGenerator
        .generateFilters(timeSeriesQueryMetaInfo.getDruidFilterMetaInfo(), botRef, customerId);

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
    JsonArray response = druidQueryExecutor.getResponseFromDruid(query, botRef, customerId);
    SimpleResponse simpleResponse = SimpleResponse.builder()
        .queryResponse(druidResponseParser.convertJsonToMap(response, botRef, customerId))
        .build();
    simpleResponse.setType(ResponseType.SIMPLE);

    prevResponse = mergePreviousResponse(simpleResponse, (SimpleResponse) prevResponse);
    return prevResponse;
  }

  private SimpleResponse mergePreviousResponse(SimpleResponse response,
      SimpleResponse prevResponse) {
    if (Objects.isNull(prevResponse) || Objects.isNull(prevResponse.getQueryResponse())
        || prevResponse.getQueryResponse().isEmpty()) {
      return response;
    } else {
      for (int resultIndex = 0; resultIndex < response.getQueryResponse().size(); resultIndex++) {
        for (int index = 0; index < response.getQueryResponse().get(resultIndex).size(); index++) {
          prevResponse.getQueryResponse().get(resultIndex).get(index)
              .putAll(response.getQueryResponse().get(resultIndex).get(index));
        }
      }
    }
    return prevResponse;
  }
}
