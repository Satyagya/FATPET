package com.engati.data.analytics.engine.service.impl;

import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.handle.metric.factory.MetricHandlerFactory;
import com.engati.data.analytics.engine.handle.query.factory.QueryHandlerFactory;
import com.engati.data.analytics.engine.ingestion.IngestionHandlerService;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.druid.query.DruidQueryMetaInfo;
import com.engati.data.analytics.sdk.druid.query.DruidQueryType;
import com.engati.data.analytics.sdk.druid.query.MultiQueryMetaInfo;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.engati.data.analytics.sdk.response.DruidTaskInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.Interval;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.engati.data.analytics.engine.constants.DruidConstants.DRUID_COMPLETE_STATUS;

@Service
@Slf4j
public class DataAnalyticsServiceImpl implements DataAnalyticsService {

  @Autowired
  private QueryHandlerFactory queryHandlerFactory;

  @Autowired
  private MetricHandlerFactory metricHandlerFactory;

  @Autowired
  private DruidQueryExecutor druidQueryExecutor;

  @Autowired
  private IngestionHandlerService ingestionHandlerService;

  @Autowired
  private DruidServiceRetrofit druidServiceRetrofit;

  @Value("${druid.ingestion.tasks.interval}")
  private Long druidIngestionTasksInterval;


  @Override
  public QueryResponse executeQueryRequest(Integer botRef, Integer customerId,
      QueryGenerationRequest request) {
    QueryResponse response = new QueryResponse();
    for (DruidQueryMetaInfo druidQueryMetaInfo: request.getQueriesMetaInfo()) {
      if (DruidQueryType.MULTI_DATA_SOURCE.name().equals(druidQueryMetaInfo.getType())) {
        String metricHandlerKey = getMetricHandlerKey(druidQueryMetaInfo);
        response = metricHandlerFactory.getMetricHandler(metricHandlerKey, botRef, customerId)
            .generateAndExecuteQuery(botRef, customerId, druidQueryMetaInfo, response);
      } else {
        response = queryHandlerFactory.getQueryHandler(druidQueryMetaInfo.getType(),
            botRef, customerId).generateAndExecuteQuery(botRef, customerId,
            druidQueryMetaInfo, response);
      }
    }
    return response;
  }

  @Override
  public DataAnalyticsEngineResponse<String> executeDruidSql(Long botRef, Long customerId,
      String druidSql) {
    return druidQueryExecutor.getDruidSqlResponse(customerId, botRef, druidSql);
  }

  @Override
  public DataAnalyticsEngineResponse<DruidIngestionResponse> ingestToDruid(Long customerId,
      Long botRef, String timestamp, String dataSourceName, Boolean isInitialLoad) {
    return ingestionHandlerService
        .ingestToDruid(customerId, botRef, timestamp, dataSourceName, isInitialLoad);
  }

  @Override
  public DataAnalyticsEngineResponse<List<DruidTaskInfo>> ingestionTaskListResponse() {
    List<DruidTaskInfo> druidTaskInfoList = Collections.emptyList();
    DataAnalyticsEngineResponse<List<DruidTaskInfo>> dataAnalyticsEngineResponse =
        new DataAnalyticsEngineResponse<>(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    dataAnalyticsEngineResponse.setResponseObject(druidTaskInfoList);
    try {
      Response<JsonArray> response;
      response =
          druidServiceRetrofit.getAllIngestionTasks(DRUID_COMPLETE_STATUS, getInterval()).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        log.info("Ingestion tasks response from druid: {}", response.body());
        String json = response.body().toString();
        ObjectMapper objectMapper = new ObjectMapper();
        druidTaskInfoList = objectMapper.readValue(json, new TypeReference<List<DruidTaskInfo>>() {
        });
        dataAnalyticsEngineResponse.setResponseObject(druidTaskInfoList);
        dataAnalyticsEngineResponse.setStatus(DataAnalyticsEngineStatusCode.SUCCESS);
      } else {
        log.info("Failed to get ingestion tasks response from druid with responseCode:{}",
            response.code());
      }
    } catch (IOException e) {
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return dataAnalyticsEngineResponse;
  }

  private String getMetricHandlerKey(DruidQueryMetaInfo druidQueryMetaInfo) {
    String metricHandlerKey = null;
    if (druidQueryMetaInfo instanceof MultiQueryMetaInfo) {
      MultiQueryMetaInfo multiQueryMetaInfo = (MultiQueryMetaInfo) druidQueryMetaInfo;
      metricHandlerKey = multiQueryMetaInfo.getMetricName();
    }
    return metricHandlerKey;
  }

  private String getInterval() {
    return new Interval(System.currentTimeMillis() - druidIngestionTasksInterval,
        System.currentTimeMillis()).toString();
  }
}
