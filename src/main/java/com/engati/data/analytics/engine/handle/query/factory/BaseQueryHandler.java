package com.engati.data.analytics.engine.handle.query.factory;

import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;

public interface BaseQueryHandler {

  /**
   * get metric handler name to generate and execute query
   *
   * @return String: metric name
   */
  String getQueryType();

  /**
   * Create simple druid query and execute query for given request
   *
   * @param druidQueryMetaInfo
   * @param botRef
   * @param customerId
   * @param prevResponse
   *
   * @return QueryResponse: result after executing the query
   */
  QueryResponse generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo, QueryResponse prevResponse);
}
