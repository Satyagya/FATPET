package com.engati.data.analytics.engine.handle.query.factory;

import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;

import java.util.List;
import java.util.Map;

public interface BaseQueryHandler {
  String getQueryType();
  List<List<Map<String, String>>> generateAndExecuteQuery(Integer botRef, Integer customerId,
      DruidQueryMetaInfo druidQueryMetaInfo);
}
