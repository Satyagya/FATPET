package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.entity.CustomerSegmentationConfiguration;
import com.engati.data.analytics.engine.model.request.CustomerSegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationConfigurationResponse;
import com.engati.data.analytics.engine.service.CustomerSegmentationConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;




@Slf4j
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@RestController
public class CustomerSegmentationConfigurationController {

  @Autowired
  private CustomerSegmentationConfigurationService customerSegmentationConfigurationService;

  @GetMapping(value = ApiPathConstants.API_BASE_PATH_FOR_SEGMENT_DETAILS)
  public ResponseEntity<DataAnalyticsResponse> getSegmentConfig(
      @PathVariable(name = ApiPathConstants.CUSTOMERID) Long customerId,
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef,
      @PathVariable(name = ApiPathConstants.SEGMENT_NAME) String segmentName) {
    log.info("Got call for getSegmentConfig for customerId: {}, botRef: {}, segmentName: {}", customerId, botRef, segmentName);
    DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> response =
        customerSegmentationConfigurationService.getConfigByBotRefAndSegment(customerId, botRef, segmentName);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.API_BASE_PATH_FOR_SEGMENT_DETAILS)
  public ResponseEntity<DataAnalyticsResponse> updateSegmentConfig(
      @RequestBody CustomerSegmentationConfigurationRequest customerSegmentationConfigurationRequest,
      @PathVariable(name = ApiPathConstants.CUSTOMERID) Long customerId,
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef,
      @PathVariable(name = ApiPathConstants.SEGMENT_NAME) String segmentName) {
    log.info("Got call for updateSegmentConfig for customerId: {}, botRef: {}, segmentName: {} and RequestBody",
        customerId, botRef, segmentName, customerSegmentationConfigurationRequest);
    DataAnalyticsResponse<CustomerSegmentationConfiguration> response =
        customerSegmentationConfigurationService.updateConfigByBotRefAndSegment(customerId, botRef, segmentName, customerSegmentationConfigurationRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }


}

