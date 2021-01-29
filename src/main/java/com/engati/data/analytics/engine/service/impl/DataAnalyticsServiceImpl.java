package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataAnalyticsServiceImpl implements DataAnalyticsService {

  @Override
  public QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request) {
    for (DruidQueryMetaInfo druidQueryMetaInfo: request.getQueriesMetaInfo()) {
      
    }
    return null;
  }
}
