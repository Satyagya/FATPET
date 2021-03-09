package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TopNQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.dimension.SimpleDimension;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.aggregation.DruidTopNQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class TopNQuery extends QueryHandler {

  private static final String QUERY_TYPE = "TOPN";

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
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    try {
      TopNQueryMetaInfo topNQueryMetaInfo = ((TopNQueryMetaInfo) druidQueryMetaInfo);
      List<DruidAggregator> druidAggregators = druidQueryGenerator
          .generateAggregators(topNQueryMetaInfo.getDruidAggregateMetaInfo(), botRef, customerId);
      List<DruidPostAggregator> postAggregators = druidQueryGenerator
          .generatePostAggregator(topNQueryMetaInfo.getDruidPostAggregateMetaInfo(), botRef,
              customerId);
      DruidFilter druidFilter = druidQueryGenerator
          .generateFilters(topNQueryMetaInfo.getDruidFilterMetaInfo(), botRef, customerId);

      DruidTopNQuery topNQuery = DruidTopNQuery.builder()
          .dataSource(Utility.convertDataSource(botRef, customerId,
              topNQueryMetaInfo.getDataSource()))
          .intervals(Utility.extractInterval(topNQueryMetaInfo.getIntervals()))
          .granularity(Utility.getGranularity(topNQueryMetaInfo.getGrain()))
          .dimension(new SimpleDimension(topNQueryMetaInfo.getDimension()))
          .threshold(topNQueryMetaInfo.getThreshold())
          .topNMetric(Utility.getMetric(topNQueryMetaInfo.getMetricType(),
              topNQueryMetaInfo.getMetricValue()))
          .aggregators(druidAggregators)
          .postAggregators(postAggregators)
          .filter(druidFilter)
          .build();

      String query = Utility.convertDruidQueryToJsonString(topNQuery);
      JsonArray response = druidQueryExecutor.getResponseFromDruid(query, botRef, customerId);
      SimpleResponse simpleResponse = SimpleResponse.builder()
          .queryResponse(druidResponseParser.convertJsonToMap(response, botRef, customerId)).build();
      simpleResponse.setType(ResponseType.SIMPLE.name());
      return simpleResponse;
    } catch (Exception ex) {
      log.error("Error while executing the topN query: {} for botRef: {}, customerId: {}, "
              + "prevResponse: {}", druidQueryMetaInfo, botRef, customerId, prevResponse, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.QUERY_FAILURE);
    }
  }
}
