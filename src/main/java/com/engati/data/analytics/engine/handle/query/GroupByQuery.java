package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.GroupByQueryMetaInfo;
import com.engati.data.analytics.sdk.response.GroupByResponse;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.google.gson.JsonArray;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.dimension.DruidDimension;
import in.zapr.druid.druidry.dimension.SimpleDimension;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import in.zapr.druid.druidry.query.aggregation.DruidGroupByQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GroupByQuery extends QueryHandler {

  private static final String QUERY_TYPE = "GROUP_BY";

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
      GroupByQueryMetaInfo groupByQueryMetaInfo = ((GroupByQueryMetaInfo) druidQueryMetaInfo);
      List<DruidAggregator> druidAggregators = druidQueryGenerator
          .generateAggregators(groupByQueryMetaInfo.getDruidAggregateMetaInfo(), botRef, customerId);
      List<DruidPostAggregator> postAggregators = druidQueryGenerator
          .generatePostAggregator(groupByQueryMetaInfo.getDruidPostAggregateMetaInfo(), botRef,
              customerId);
      DruidFilter druidFilter = druidQueryGenerator
          .generateFilters(groupByQueryMetaInfo.getDruidFilterMetaInfo(), botRef, customerId);

      DruidGroupByQuery groupByQuery = DruidGroupByQuery.builder().dataSource(
          Utility.convertDataSource(botRef, customerId, groupByQueryMetaInfo.getDataSource()))
          .intervals(Utility.extractInterval(groupByQueryMetaInfo.getIntervals()))
          .granularity(Utility.getGranularity(groupByQueryMetaInfo.getGrain()))
          .dimensions(Utility.getDimension(groupByQueryMetaInfo.getDimension()))
          .aggregators(druidAggregators).postAggregators(postAggregators).filter(druidFilter)
          .build();

      String query = Utility.convertDruidQueryToJsonString(groupByQuery);
      JsonArray response = druidQueryExecutor.getResponseFromDruid(query, botRef, customerId);
      GroupByResponse groupByResponse = GroupByResponse.builder().groupByResponse(
          druidResponseParser.convertGroupByJsonToMap(response, botRef, customerId)).build();
      groupByResponse.setType(ResponseType.GROUP_BY.name());
      return groupByResponse;
    } catch (Exception ex) {
      log.error("Error while executing the groupBy query: {} for botRef: {}, customerId: {}, "
          + "prevResponse: {}", druidQueryMetaInfo, botRef, customerId, prevResponse, ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.QUERY_FAILURE);
    }
  }
}
