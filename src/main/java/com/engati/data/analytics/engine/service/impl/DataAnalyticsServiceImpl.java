package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.handle.metric.factory.MetricHandlerFactory;
import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.ingestionHandler.IngestionHandlerService;
import com.engati.data.analytics.engine.response.ingestion.DruidIngestionResponse;
import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryType;
import com.engati.data.analytics.sdk.druid.query.GroupByQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class DataAnalyticsServiceImpl implements DataAnalyticsService {

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Autowired
  private MetricHandlerFactory metricHandlerFactory;

  @Autowired
  private DruidQueryExecutor druidQueryExecutor;

  @Autowired
  IngestionHandlerService ingestionHandlerService;

  @Override
  public QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request) {
    QueryResponse response = new QueryResponse();
    for (DruidQueryMetaInfo druidQueryMetaInfo: request.getQueriesMetaInfo()) {
      if (DruidQueryType.MULTI_DATA_SOURCE.equals(druidQueryMetaInfo.getQueryType())) {
        String metricHandlerKey = getMetricHandlerKey(druidQueryMetaInfo);
        response = metricHandlerFactory.getMetricHandler(metricHandlerKey)
            .generateAndExecuteQuery(botRef, customerId, druidQueryMetaInfo, response);
      } else {
        response = queryHandlerFactory.getQueryHandler(druidQueryMetaInfo.getQueryType().name())
                .generateAndExecuteQuery(botRef, customerId, druidQueryMetaInfo, response);
      }
    }
    return response;
  }

  @Override
  public DataAnalyticsEngineResponse<String> executeDruidSql(Long botRef, Long customerId, String druidSql) {
    return druidQueryExecutor.getDruidSqlResponse(customerId,botRef,druidSql);
  }

  @Override
  public DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId,
      Long botRef, String timestamp, String dataSourceName, Boolean isInitialLoad) {
    return ingestionHandlerService.ingestToDruid(customerId,botRef,timestamp,dataSourceName,isInitialLoad);
  }

  private List<List<Map<String, String>>> mergePreviousResponse(List<List<Map<String, String>>>
      response, List<List<Map<String, String>>> prevResponse) {
    if (Objects.isNull(prevResponse) || prevResponse.isEmpty()) {
      return response;
    } else {
      for (int resultIndex = 0; resultIndex < response.size(); resultIndex++) {
        for (int index = 0; index < response.get(resultIndex).size(); index++) {
          prevResponse.get(resultIndex).get(index).putAll(response.get(resultIndex).get(index));
        }
      }
    }
    return prevResponse;
  }

  private String getMetricHandlerKey(DruidQueryMetaInfo druidQueryMetaInfo) {
    String metricHandlerKey = null;
    if (druidQueryMetaInfo instanceof MultiQueryMetaInfo) {
      MultiQueryMetaInfo multiQueryMetaInfo = (MultiQueryMetaInfo) druidQueryMetaInfo;
      metricHandlerKey = multiQueryMetaInfo.getMetricName();
    }
    return metricHandlerKey;
  }
}
