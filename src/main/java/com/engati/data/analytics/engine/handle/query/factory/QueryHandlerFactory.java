package com.engati.data.analytics.engine.handle.query.factory;

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

  public BaseQueryHandler getQueryHandler(String type) {
    if (QUERY_HANDLER_MAP.containsKey(type)) {
      return QUERY_HANDLER_MAP.get(type);
    }
    log.error("Metric type is not available");
    return null;
  }
}
