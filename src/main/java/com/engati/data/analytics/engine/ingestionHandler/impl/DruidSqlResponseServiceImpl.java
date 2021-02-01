package com.engati.data.analytics.engine.ingestionHandler.impl;

import com.engati.data.analytics.engine.ingestionHandler.DruidSqlResponseService;
import com.engati.data.analytics.engine.retrofit.DruidServiceRetrofit;
import com.google.gson.JsonArray;
import lombok.extern.slf4j.Slf4j;
import okhttp3.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
   *
   * @param customerId    : customer identifier
   * @param botRef        : bot reference
   * @param druidSqlQuery : dql query
   *
   * @return
   */
  @Override
  public ResponseEntity<JsonArray> getDruidSqlResponse(Long customerId, Long botRef,
      String druidSqlQuery) {
    JsonArray output = new JsonArray();
    ResponseEntity<JsonArray> responseEntity = null;
    try {
      RequestBody body = okhttp3.RequestBody
          .create(okhttp3.MediaType.parse("application/json; charset=utf-8"), druidSqlQuery);
      Response<JsonArray> response;
      response = druidServiceRetrofit.getResponseForDruidSqlFromDruid(body).execute();
      if (Objects.nonNull(response) && Objects.nonNull(response.body()) && response
          .isSuccessful()) {
        log.info("response: {}", response);
        output = response.body();
        responseEntity = new ResponseEntity<>(output, HttpStatus.valueOf(response.code()));
      } else {
        log.error("Failed to get response errorBody:{}", response.errorBody().string());
        responseEntity = new ResponseEntity<>(output, HttpStatus.valueOf(response.code()));
      }
    } catch (IOException e) {
      responseEntity = new ResponseEntity<>(output, HttpStatus.FORBIDDEN);
      log.error("Failed to get response", e);
    }
    return responseEntity;
  }
}