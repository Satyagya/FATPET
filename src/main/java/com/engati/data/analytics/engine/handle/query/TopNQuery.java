package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.druid.query.service.DruidQueryGenerator;
import com.engati.data.analytics.engine.druid.response.DruidResponseParser;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.util.Utility;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.TopNQueryMetaInfo;
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
import java.util.Map;

@Slf4j
@Component
public class TopNQuery extends QueryHandler {

  private static final String QUERY_TYPE_TOPN = "topN";

  @Autowired
  private DruidQueryGenerator druidQueryGenerator;

  @Autowired
  private DruidQueryExecutor druidQueryExecutor;

  @Autowired
  private DruidResponseParser druidResponseParser;

  @Override
  public String getQueryType() {
    return QUERY_TYPE_TOPN;
  }

  @Override
  public List<List<Map<String, String>>> generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo) {

    TopNQueryMetaInfo topNQueryMetaInfo = ((TopNQueryMetaInfo) druidQueryMetaInfo);

    List<DruidAggregator> druidAggregators = druidQueryGenerator
        .generateAggregators(topNQueryMetaInfo.getDruidAggregateMetaInfo());
    List<DruidPostAggregator> postAggregators = druidQueryGenerator
        .generatePostAggregator(topNQueryMetaInfo.getDruidPostAggregateMetaInfo());
    DruidFilter druidFilter = druidQueryGenerator
        .generateFilters(topNQueryMetaInfo.getDruidFilterMetaInfo());

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
    JsonArray response = druidQueryExecutor.getResponseFromDruid(query);
    return druidResponseParser.convertJsonToMap(response);
  }
}
