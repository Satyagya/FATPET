package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;

public interface DataAnalyticsService {
  QueryResponse executeQueryRequest(Integer botRef, Integer customerId, QueryGenerationRequest request);
}
