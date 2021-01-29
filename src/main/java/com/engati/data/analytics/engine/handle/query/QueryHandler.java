package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.handle.query.factory.BaseQueryHandler;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;

import java.util.List;
import java.util.Map;

abstract class QueryHandler implements BaseQueryHandler {
  @Override
  public String getQueryType() {
    return null;
  }

  @Override
  public List<List<Map<String, String>>> generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo) {
    return null;
  }
}
