package com.engati.data.analytics.engine.handle.metric.factory;

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
public class MetricHandlerFactory {

  private static final Map<String, BaseMetricHandler> METRIC_HANDLER_MAP = new HashMap<>();

  @Autowired
  private List<BaseMetricHandler> metricHandlers;

  @PostConstruct
  public void init() {
    for (BaseMetricHandler metricHandler : metricHandlers) {
      METRIC_HANDLER_MAP.put(metricHandler.getMetricName(), metricHandler);
    }
  }

  public BaseMetricHandler getMetricHandler(String type, Integer botRef, Integer customerId) {
    if (METRIC_HANDLER_MAP.containsKey(type)) {
      return METRIC_HANDLER_MAP.get(type);
    }
    log.error("Metric type: {} is not implemented for botRef: {}, customerId: {}", type,
        botRef, customerId);
    throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
  }
}
