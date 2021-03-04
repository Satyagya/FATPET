package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.druidry.join.DruidJoinTopN;
import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryType;
import com.engati.data.analytics.sdk.druid.query.join.JoinTopNMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import com.google.gson.JsonArray;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.dimension.DefaultDimension;
import in.zapr.druid.druidry.dimension.DruidDimension;
import in.zapr.druid.druidry.filter.DruidFilter;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class JoinTopNQuery extends QueryHandler {

  @Autowired
  private DruidQueryGenerator druidQueryGenerator;

  @Autowired
  private DruidQueryExecutor druidQueryExecutor;

  @Autowired
  private DruidResponseParser druidResponseParser;

  @Override
  public String getQueryType() {
    return DruidQueryType.JOIN_TOPN.name();
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {

    JoinTopNMetaInfo joinTopNMetaInfo = ((JoinTopNMetaInfo) druidQueryMetaInfo);

    List<DruidAggregator> druidAggregators = druidQueryGenerator
        .generateAggregators(joinTopNMetaInfo.getDruidAggregateMetaInfo(),
            botRef, customerId);
    List<DruidPostAggregator> postAggregators = druidQueryGenerator
        .generatePostAggregator(joinTopNMetaInfo.getDruidPostAggregateMetaInfo(),
            botRef, customerId);
    DruidFilter druidFilter = druidQueryGenerator
        .generateFilters(joinTopNMetaInfo.getDruidFilterMetaInfo(),
            botRef, customerId);

    DruidJoinTopN topNQuery = DruidJoinTopN.builder()
        .dataSource(Utility.getDruidJoin(joinTopNMetaInfo.getDataSource(),
            botRef, customerId))
        .intervals(Utility.extractInterval(joinTopNMetaInfo.getIntervals()))
        .granularity(Utility.getGranularity(joinTopNMetaInfo.getGrain()))
        .dimension(getDruidDimension(joinTopNMetaInfo.getDimension(),
            joinTopNMetaInfo.getDataSource().getRightPrefix()))
        .threshold(joinTopNMetaInfo.getThreshold())
        .topNMetric(Utility.getMetric(joinTopNMetaInfo.getMetricType(),
            joinTopNMetaInfo.getMetricValue()))
        .aggregators(druidAggregators)
        .postAggregators(postAggregators)
        .filter(druidFilter)
        .build();

    String query = Utility.convertDruidQueryToJsonString(topNQuery);
    JsonArray response = druidQueryExecutor.getResponseFromDruid(query, botRef, customerId);
    SimpleResponse simpleResponse = SimpleResponse.builder()
        .queryResponse(druidResponseParser.convertJsonToMap(response, botRef, customerId))
        .build();
    simpleResponse.setType(ResponseType.SIMPLE.name());
    return simpleResponse;
  }

  private DruidDimension getDruidDimension(String dimension, String prefix) {
     return DefaultDimension.builder().dimension(prefix.concat(dimension))
         .outputName(dimension).build();
  }
}
