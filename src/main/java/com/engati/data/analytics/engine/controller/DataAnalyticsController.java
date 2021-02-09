package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.QueryResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
@Slf4j
public class DataAnalyticsController {

  @Autowired
  private DataAnalyticsService dataAnalyticsService;

  @PostMapping(value = Constants.DRUID_RESPONSE_API_PATH)
  public ResponseEntity<DataAnalyticsEngineResponse<QueryResponse>> getDruidResponse(
      @PathVariable(Constants.REQ_PARAM_CUSTOMER_ID) Integer customerId,
      @PathVariable(Constants.REQ_PARAM_BOT_REF) Integer botRef,
      @RequestBody QueryGenerationRequest queryGenerationRequest) {
    QueryResponse responseFromDruid = dataAnalyticsService.executeQueryRequest(botRef, customerId,
        queryGenerationRequest);
    DataAnalyticsEngineResponse<QueryResponse> response =
        new DataAnalyticsEngineResponse<>(responseFromDruid, DataAnalyticsEngineStatusCode.SUCCESS,
            HttpStatus.OK);
    return new ResponseEntity<>(response, response.getStatusCode());
  }


}
