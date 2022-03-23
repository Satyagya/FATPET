package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.entity.StoreSegmentationConfiguration;
import com.engati.data.analytics.engine.model.request.SegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;
import com.engati.data.analytics.engine.service.StoreSegmentationConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@RestController
public class StoreSegmentationConfigurationController {

  @Autowired
  private StoreSegmentationConfigurationService storeSegmentationConfigurationService;

  @GetMapping(value = ApiPathConstants.API_BASE_PATH_FOR_SEGMENT_DETAILS)
  public ResponseEntity<DataAnalyticsResponse> getSegmentConfig(
      @PathVariable(name = ApiPathConstants.CUSTOMERID) Long customerId,
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef,
      @PathVariable(name = ApiPathConstants.SEGMENT_NAME) String segmentName) {
    log.info("Got call for getSegmentConfig for customerId: {}, botRef: {}, segmentName: {}", customerId, botRef, segmentName);
    DataAnalyticsResponse<SegmentationConfigurationResponse> response =
        storeSegmentationConfigurationService.getConfigByBotRefAndSegment(customerId, botRef, segmentName);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.API_BASE_PATH_FOR_SEGMENT_DETAILS)
  public ResponseEntity<DataAnalyticsResponse> updateSegmentConfig(
      @RequestBody SegmentationConfigurationRequest segmentationConfigurationRequest,
      @PathVariable(name = ApiPathConstants.CUSTOMERID) Long customerId,
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef,
      @PathVariable(name = ApiPathConstants.SEGMENT_NAME) String segmentName) {
    log.info("Got call for updateSegmentConfig for customerId: {}, botRef: {}, segmentName: {}",
        customerId, botRef, segmentName);
    DataAnalyticsResponse<SegmentationConfigurationResponse> response =
        storeSegmentationConfigurationService.updateConfigByBotRefAndSegment(customerId, botRef, segmentName, segmentationConfigurationRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }


}

