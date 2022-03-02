package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.SegmentConfigResponse;
import com.engati.data.analytics.engine.model.request.SegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;
import com.engati.data.analytics.engine.service.StoreSegmentationConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1")
@Slf4j
public class StoreSegmentationConfigurationController {

  @Autowired
  private StoreSegmentationConfigurationService storeSegmentationConfigurationService;

  @RequestMapping(value = "/customerId/{customerId}/botRef/{botRef}/segment/{segmentName}/getConfig", method = RequestMethod.GET)
  public ResponseEntity<SegmentConfigResponse> getSegmentConfig (
      @PathVariable(name = "customerId") Long customerId,
      @PathVariable(name = "botRef") Long botRef,
      @PathVariable(name="segmentName") String segmentName
  ) {
    log.info("Got call for getSegmentConfig for customerId: {}, botRef: {}, segmentName: {}", customerId, botRef, segmentName);
    SegmentConfigResponse<SegmentationConfigurationResponse> response =
        storeSegmentationConfigurationService.getConfigByBotRefAndSegment(customerId, botRef, segmentName);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @RequestMapping(value = "/updateConfig", method = RequestMethod.POST)
  public ResponseEntity<SegmentConfigResponse> updateSegmentConfig (
      @RequestBody SegmentationConfigurationRequest segmentationConfigurationRequest
      ){
    log.info("Got call for updateSegmentConfig for customerId: {}, botRef: {}, segmentName: {}",
        segmentationConfigurationRequest.getCustomerId(), segmentationConfigurationRequest.getBotRef(), segmentationConfigurationRequest.getSegmentName());
    SegmentConfigResponse<SegmentationConfigurationResponse> response =
        storeSegmentationConfigurationService.updateConfigByBotRefAndSegment(segmentationConfigurationRequest.getCustomerId(), segmentationConfigurationRequest.getBotRef(), segmentationConfigurationRequest.getSegmentName(), segmentationConfigurationRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }


}

