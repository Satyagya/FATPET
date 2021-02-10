package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.handle.metric.factory.MetricHandlerFactory;
import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryType;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataAnalyticsServiceImpl implements DataAnalyticsService {

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Autowired
  private MetricHandlerFactory metricHandlerFactory;

  @Override
  public QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request) {
    QueryResponse response = new QueryResponse();
    for (DruidQueryMetaInfo druidQueryMetaInfo: request.getQueriesMetaInfo()) {
      if (DruidQueryType.MULTI_DATA_SOURCE.equals(druidQueryMetaInfo.getQueryType())) {
        String metricHandlerKey = getMetricHandlerKey(druidQueryMetaInfo);
        response = metricHandlerFactory.getMetricHandler(metricHandlerKey, botRef, customerId)
            .generateAndExecuteQuery(botRef, customerId, druidQueryMetaInfo, response);
      } else {
        response = queryHandlerFactory.getQueryHandler(druidQueryMetaInfo.getQueryType().name(),
            botRef, customerId).generateAndExecuteQuery(botRef, customerId,
            druidQueryMetaInfo, response);
      }
    }
    return response;
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
