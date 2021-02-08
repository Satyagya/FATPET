package com.engati.data.analytics.engine.handle.metric.factory;

import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

public interface BaseMetricHandler {
  String getMetricName();
  QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse);
}
