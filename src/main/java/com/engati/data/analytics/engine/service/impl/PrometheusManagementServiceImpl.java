package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.constants.constant.Constants;
import com.engati.data.analytics.engine.service.PrometheusManagementService;
import io.micrometer.core.instrument.Metrics;
import org.springframework.stereotype.Service;

@Service("com.engati.data.analytics.engine.service.impl.PrometheusManagementServiceImpl")
public class PrometheusManagementServiceImpl implements PrometheusManagementService {

  @Override
  public void apiRequestFailureEvent(String event, String botRef, String causeOfFailure,
      String apiRequest) {
    Metrics.counter(Constants.PROMETHEUS_API_REQUEST_COUNTER_NAME,
        Constants.PROMETHEUS_BOT_REF, botRef,
        Constants.PROMETHEUS_SHOPIFY_EVENT, event,
        Constants.PROMETHEUS_CAUSE_OF_FAILURE, causeOfFailure,
        Constants.PROMETHEUS_REQUEST_PAYLOAD, apiRequest).increment();
  }
}
