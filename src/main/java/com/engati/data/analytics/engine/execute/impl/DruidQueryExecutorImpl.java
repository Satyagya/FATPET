package com.engati.data.analytics.engine.execute.impl;

import com.engati.data.analytics.engine.execute.DruidQueryExecutor;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Objects;

@Service
@Slf4j
public class DruidQueryExecutorImpl implements DruidQueryExecutor {

  @Autowired
  private DruidServiceRetrofit druidServiceRetrofit;

  @Override
  public JsonArray getResponseFromDruid(String druidJsonQuery) {
    JsonArray output = new JsonArray();
    try {
      okhttp3.RequestBody body = okhttp3.RequestBody
          .create(okhttp3.MediaType.parse("application/json; charset=utf-8"), druidJsonQuery);
      retrofit2.Response<JsonArray> response;
      response =
          druidServiceRetrofit.getResponseFromDruid(body).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body())
          && response.isSuccessful()) {
        log.info("response: {}, body: {}", response, response.body());
        output = response.body();
      } else {
        log.error("Failed to get response errorBody:{}", response.errorBody().toString());
      }
    } catch (IOException e) {
      log.error("Failed to get response", e);
    }
    return output;
  }
}
