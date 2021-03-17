package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.druidry.join.DruidJoinTimeSeries;
import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryType;
import com.engati.data.analytics.sdk.druid.query.join.JoinTimeSeriesMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class JoinTimeSeriesQuery extends QueryHandler {

  @Autowired
  private DruidQueryGenerator druidQueryGenerator;

  @Autowired
  private DruidQueryExecutor druidQueryExecutor;

  @Autowired
  private DruidResponseParser druidResponseParser;

  @Override
  public String getQueryType() {
    return DruidQueryType.JOIN_TIME_SERIES.name();
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer
      customerId, DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    try {
      JoinTimeSeriesMetaInfo joinTimeSeriesMetaInfo = ((JoinTimeSeriesMetaInfo)
          druidQueryMetaInfo);
      List<DruidAggregator> druidAggregators = druidQueryGenerator
          .generateAggregators(joinTimeSeriesMetaInfo.getDruidAggregateMetaInfo(),
              botRef, customerId);
      List<DruidPostAggregator> postAggregators = druidQueryGenerator
          .generatePostAggregator(joinTimeSeriesMetaInfo.getDruidPostAggregateMetaInfo(),
              botRef, customerId);
      DruidFilter druidFilter = druidQueryGenerator
          .generateFilters(joinTimeSeriesMetaInfo.getDruidFilterMetaInfo(), botRef, customerId);

      DruidJoinTimeSeries timeSeriesQuery = DruidJoinTimeSeries.builder()
          .dataSource(druidQueryGenerator.getJoinDataSource(joinTimeSeriesMetaInfo.getDataSource(),
              botRef, customerId))
          .intervals(Utility.extractInterval(joinTimeSeriesMetaInfo.getIntervals()))
          .aggregators(druidAggregators)
          .postAggregators(postAggregators)
          .filter(druidFilter)
          .granularity(Utility.getGranularity(joinTimeSeriesMetaInfo.getGrain()))
          .build();

      String query = Utility.convertDruidQueryToJsonString(timeSeriesQuery);
      JsonArray response = druidQueryExecutor.getResponseFromDruid(query, botRef, customerId);
      SimpleResponse simpleResponse = SimpleResponse.builder()
          .queryResponse(druidResponseParser.convertJsonToMap(response, botRef, customerId))
          .build();
      simpleResponse.setType(ResponseType.SIMPLE.name());
      prevResponse = (prevResponse instanceof SimpleResponse) ?
          druidResponseParser.mergePreviousResponse(simpleResponse, (SimpleResponse) prevResponse)
          : simpleResponse;
      return prevResponse;
    } catch (Exception ex) {
      log.error("Exception while executing the join-timeseries query: {} for botRef: {},"
          + " customerId: {}, prevResponse: {}", druidQueryMetaInfo, botRef, customerId,
          prevResponse, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.QUERY_FAILURE);
    }
  }
}
