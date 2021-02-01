package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class DataAnalyticsServiceImpl implements DataAnalyticsService {

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Override
  public QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request) {
    List<List<Map<String, String>>> prevResponse = new ArrayList<>();
    for (DruidQueryMetaInfo druidQueryMetaInfo: request.getQueriesMetaInfo()) {
      List<List<Map<String, String>>> response = queryHandlerFactory
          .getQueryHandler(druidQueryMetaInfo.getQueryType().name())
          .generateAndExecuteQuery(botRef, customerId, druidQueryMetaInfo);
      prevResponse = mergePreviousResponse(response, prevResponse);
    }
    return QueryResponse.builder().queryResponse(prevResponse).build();
  }

  private List<List<Map<String, String>>> mergePreviousResponse(List<List<Map<String, String>>>
      response, List<List<Map<String, String>>> prevResponse) {
    if (Objects.isNull(prevResponse) || prevResponse.isEmpty()) {
      return response;
    } else {
      for (int resultIndex = 0; resultIndex < response.size(); resultIndex++) {
        for (int index = 0; index < response.get(resultIndex).size(); index++) {
          prevResponse.get(resultIndex).get(index).putAll(response.get(resultIndex).get(index));
        }
      }
    }
    return prevResponse;
  }
}
