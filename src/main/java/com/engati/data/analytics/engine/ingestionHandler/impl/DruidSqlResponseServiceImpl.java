package com.engati.data.analytics.engine.ingestionHandler.impl;

import com.engati.data.analytics.engine.ingestionHandler.DruidSqlResponseService;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import okhttp3.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Response;

import java.io.IOException;
import java.util.Objects;

@Slf4j
@Service("com.engati.data.analytics.engine.ingestionHandler.impl.DruidSqlResponseServiceImpl")
public class DruidSqlResponseServiceImpl implements DruidSqlResponseService {

  @Autowired
  DruidServiceRetrofit druidServiceRetrofit;

  /**
   * returns response from druid for the provided dql query
   * @param customerId : customer identifier
   * @param botRef : bot reference
   * @param druidSqlQuery : dql query
   * @return
   */
  @Override
  public String getDruidSqlResponse(Long customerId, Long botRef,
      String druidSqlQuery) {
    String output = null;
    try {
      RequestBody body = okhttp3.RequestBody
          .create(okhttp3.MediaType.parse("application/json;"), druidSqlQuery);
      Response<JsonArray> response;
      response = druidServiceRetrofit.getResponseForDruidSqlFromDruid(body).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        log.info("response: {}", response);
        output = response.body().toString();
      } else {
        log.error("Failed to get response errorBody:{}", response.errorBody().string());
      }
    } catch (IOException e) {
      log.error("Failed to get response", e);
    }
    return output;
  }
}