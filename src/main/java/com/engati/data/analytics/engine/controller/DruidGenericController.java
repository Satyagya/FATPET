package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.ingestionHandler.DruidSqlResponseService;
import com.engati.data.analytics.engine.ingestionHandler.IngestionHandlerService;
import com.engati.data.analytics.engine.response.ingestion.IngestionResponse;
import com.engati.data.analytics.engine.response.ingestion.UserIngestionProcess;
import com.google.gson.JsonArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class DruidGenericController {

  @Autowired
  IngestionHandlerService ingestionHandlerService;

  @Autowired
  DruidSqlResponseService druidSqlResponseService;

  @RequestMapping(value = "/customer/{customerId}/bot/{botRef}/ingest", method = RequestMethod.POST)
  public ResponseEntity<IngestionResponse<UserIngestionProcess>> ingestData(
      @PathVariable(value = "customerId") Long customerId,
      @PathVariable(value = "botRef") Long botRef,
      @RequestParam(value = "isInitialLoad", required = true) Boolean isInitialLoad,
      @RequestParam(value = "timestamp", required = true) String timestamp,
      @RequestParam(value = "dataSourceName", required = true) String dataSourceName) {
    IngestionResponse<UserIngestionProcess> responseObj = ingestionHandlerService
        .ingestToDruid(customerId, botRef, timestamp, dataSourceName, isInitialLoad);
    return new ResponseEntity<>(responseObj, responseObj.getStatusCode());
  }

  @RequestMapping(value = "/customer/{customerId}/bot/{botRef}/response",
      method = RequestMethod.POST)
  public ResponseEntity<JsonArray> ingestData(@PathVariable(value = "customerId") Long customerId,
      @PathVariable(value = "botRef") Long botRef, @RequestBody String query) {
    return druidSqlResponseService.getDruidSqlResponse(customerId, botRef, query);
  }

}
