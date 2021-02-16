package com.engati.data.analytics.engine.handle.query;

import com.engati.data.analytics.engine.handle.query.factory.BaseQueryHandler;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

import java.util.List;
import java.util.Map;

abstract class QueryHandler implements BaseQueryHandler {
  @Override
  public String getQueryType() {
    return null;
  }

  @Override
  public QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse) {
    return null;
  }
}
