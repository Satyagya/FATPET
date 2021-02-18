package com.engati.data.analytics.engine.handle.query.factory;

import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

import java.util.List;
import java.util.Map;

public interface BaseQueryHandler {
  String getQueryType();
  QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse);
}
