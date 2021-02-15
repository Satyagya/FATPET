package com.engati.data.analytics.engine.execute.impl;


import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineException;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import okhttp3.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.io.IOException;
import java.util.Objects;

@Service
@Slf4j
public class DruidQueryExecutorImpl implements DruidQueryExecutor {

  @Autowired
  private DruidServiceRetrofit druidServiceRetrofit;

  @Override
  public JsonArray getResponseFromDruid(String druidJsonQuery, Integer botRef, Integer customerId) {
    JsonArray output = new JsonArray();
    try {
      okhttp3.RequestBody body = okhttp3.RequestBody
          .create(okhttp3.MediaType.parse("application/json; charset=utf-8"), druidJsonQuery);
      retrofit2.Response<JsonArray> response;
      response = druidServiceRetrofit.getResponseFromDruid(body).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        output = response.body();
      } else {
        log.error("DruidQueryExecutorImpl: Failed to get response from druid " + "errorBody:{}",
            response.errorBody().toString());
        throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
      }
    } catch (IOException ex) {
      log.error("DruidQueryExecutorImpl: Failed to parse response from druid", ex);
      throw new DataAnalyticsEngineException(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    }
    return output;
  }

  /**
   * returns response from druid for the provided dql query
   *
   * @param customerId    : customer identifier
   * @param botRef        : bot reference
   * @param druidSqlQuery : dql query
   *
   * @return
   */
  @Override
  public DataAnalyticsEngineResponse<String> getDruidSqlResponse(Long customerId, Long botRef,
      String druidSqlQuery) {
    String output = null;
    DataAnalyticsEngineResponse<String> dataAnalyticsEngineResponse =
        new DataAnalyticsEngineResponse<>(DataAnalyticsEngineStatusCode.PROCESSING_ERROR);
    dataAnalyticsEngineResponse.setResponseObject(output);
    try {
      RequestBody body = okhttp3.RequestBody
          .create(okhttp3.MediaType.parse("application/json;charset=utf-8"), druidSqlQuery);
      Response<JsonArray> response;
      response = druidServiceRetrofit.getResponseForDruidSqlFromDruid(body).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        log.info("Response for customerId:{}, botRef:{} with query:{} is {}", customerId, botRef,
            druidSqlQuery, response);
        output = response.body().toString();
        dataAnalyticsEngineResponse.setResponseObject(output);
        dataAnalyticsEngineResponse.setStatus(DataAnalyticsEngineStatusCode.SUCCESS);
      } else {
        log.info("Failed to get response for customerId:{}, botRef:{} errorBody:{}", customerId,
            botRef, response.errorBody().string());
      }
    } catch (IOException e) {
      log.error("Failed to get response for customerId:{},botRef:{}", customerId, botRef, e);
    }
    return dataAnalyticsEngineResponse;
  }
}
