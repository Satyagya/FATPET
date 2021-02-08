package com.engati.data.analytics.engine.handle.metric;

import com.engati.data.analytics.engine.handle.metric.factory.BaseMetricHandler;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

abstract class MetricHandler implements BaseMetricHandler {

  @Override
  public String getMetricName() {
    return null;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    return null;
  }
}
