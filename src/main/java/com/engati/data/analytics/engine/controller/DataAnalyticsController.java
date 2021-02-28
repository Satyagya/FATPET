package com.engati.data.analytics.engine.controller;


import com.engati.data.analytics.engine.service.DataAnalyticsService;
import com.engati.data.analytics.engine.util.Constants;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineResponse;
import com.engati.data.analytics.sdk.common.DataAnalyticsEngineStatusCode;
import com.engati.data.analytics.sdk.request.QueryGenerationRequest;
import com.engati.data.analytics.sdk.response.DruidIngestionResponse;
import com.engati.data.analytics.sdk.response.DruidTaskInfo;
import com.engati.data.analytics.sdk.response.QueryResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static com.engati.data.analytics.engine.util.Constants.DRUID_INGESTION_API_PATH;
import static com.engati.data.analytics.engine.util.Constants.DRUID_SQL_RESPONSE_API_PATH;
import static com.engati.data.analytics.engine.util.Constants.DRUID_TASK_RESPONSE_API_PATH;

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
    log.info("DataAnalyticsController: Request to execute the druid query request: {} "
        + "for botRef: {} and customerId: {}", queryGenerationRequest, botRef, customerId);
    QueryResponse responseFromDruid = dataAnalyticsService.executeQueryRequest(botRef, customerId,
        queryGenerationRequest);
    DataAnalyticsEngineResponse<QueryResponse> response =
        new DataAnalyticsEngineResponse<>(responseFromDruid, DataAnalyticsEngineStatusCode.SUCCESS,
            HttpStatus.OK);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @RequestMapping(value = DRUID_INGESTION_API_PATH, method = RequestMethod.GET)
  public ResponseEntity<DataAnalyticsEngineResponse<DruidIngestionResponse>> ingestData(
      @PathVariable(value = Constants.REQ_PARAM_CUSTOMER_ID) Long customerId,
      @PathVariable(value = Constants.REQ_PARAM_BOT_REF) Long botRef,
      @RequestParam(value = Constants.REQ_PARAM_IS_INITIAL, required = true) Boolean isInitialLoad,
      @RequestParam(value = Constants.REQ_PARAM_TIMESTAMP, required = false) String timestamp,
      @RequestParam(value = Constants.REQ_PARAM_DATA_SOURCE_NAME, required = true)
          String dataSourceName) {
    DataAnalyticsEngineResponse<DruidIngestionResponse> dataAnalyticsEngineResponse =
        dataAnalyticsService
            .ingestToDruid(customerId, botRef, timestamp, dataSourceName, isInitialLoad);
    return new ResponseEntity<>(dataAnalyticsEngineResponse,
        dataAnalyticsEngineResponse.getStatusCode());
  }

  @RequestMapping(value = DRUID_SQL_RESPONSE_API_PATH, method = RequestMethod.POST)
  public ResponseEntity<DataAnalyticsEngineResponse<String>> getDruidSqlResponse(
      @PathVariable(value = Constants.REQ_PARAM_CUSTOMER_ID) Long customerId,
      @PathVariable(value = Constants.REQ_PARAM_BOT_REF) Long botRef, @RequestBody String query) {
    DataAnalyticsEngineResponse<String> dataAnalyticsEngineResponse =
        dataAnalyticsService.executeDruidSql(customerId, botRef, query);
    return new ResponseEntity<>(dataAnalyticsEngineResponse,
        dataAnalyticsEngineResponse.getStatusCode());
  }

  @GetMapping(value = DRUID_TASK_RESPONSE_API_PATH)
  public ResponseEntity<DataAnalyticsEngineResponse<List<DruidTaskInfo>>> getDruidTasksInfo(){
    DataAnalyticsEngineResponse<List<DruidTaskInfo>> dataAnalyticsEngineResponse = dataAnalyticsService.getAllTasks();
    return new ResponseEntity<>(dataAnalyticsEngineResponse,dataAnalyticsEngineResponse.getStatusCode());
  }
}
