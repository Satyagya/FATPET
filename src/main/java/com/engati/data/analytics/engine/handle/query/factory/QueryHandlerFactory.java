package com.engati.data.analytics.engine.handle.query.factory;

import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class QueryHandlerFactory {

  private static final Map<String, BaseQueryHandler> QUERY_HANDLER_MAP = new HashMap<>();

  @Autowired
  private List<BaseQueryHandler> metricHandlers;

  @PostConstruct
  public void init() {
    for (BaseQueryHandler metricHandler : metricHandlers) {
      QUERY_HANDLER_MAP.put(metricHandler.getQueryType(), metricHandler);
    }
  }

  public BaseQueryHandler getQueryHandler(String type, Integer botRef, Integer customerId) {
    if (QUERY_HANDLER_MAP.containsKey(type)) {
      return QUERY_HANDLER_MAP.get(type);
    }
    log.error("Query type: {} is not implemented for botRef: {}, customerId: {}", type,
        botRef, customerId);
    throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
  }
}
