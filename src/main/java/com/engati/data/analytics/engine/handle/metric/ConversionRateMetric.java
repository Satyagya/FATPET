package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.response.GroupByResponse;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.engati.data.analytics.sdk.response.ResponseType;
import com.engati.data.analytics.sdk.response.SimpleResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;

@Slf4j
@Component
public class ConversionRateMetric extends MetricHandler {

  private static final String METRIC_HANDLER_NAME = "conversion_rate";

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Override
  public String getMetricName() {
    return METRIC_HANDLER_NAME;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    List<QueryResponse> responses = new ArrayList<>();
    MultiQueryMetaInfo multiQueryMetaInfo = ((MultiQueryMetaInfo) druidQueryMetaInfo);
    for (DruidQueryMetaInfo druidQuery: multiQueryMetaInfo.getMultiMetricQuery()) {
      QueryResponse response = new QueryResponse();
      responses.add(queryHandlerFactory.getQueryHandler(druidQuery.getType(),
          botRef, customerId).generateAndExecuteQuery(botRef, customerId,
          druidQuery, response));
    }
    return responses.get(0);
  }
}
