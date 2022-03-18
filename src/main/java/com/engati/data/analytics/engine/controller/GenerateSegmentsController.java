package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.CustomerSegmentResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.service.GenerateSegmentsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;


@RestController
@RequestMapping("/v1")
@Slf4j
public class GenerateSegmentsController {

  @Autowired
  private GenerateSegmentsService generateSegmentsService;

  @RequestMapping(value = "/customerId/{customerId}/botRef/{botRef}/segment/{segmentName}/getCustomerSegment", method = RequestMethod.GET)
  public ResponseEntity<CustomerSegmentResponse> getCustomerSegment(
      @PathVariable(name = "customerId") Long customerId,
      @PathVariable(name = "botRef") Long botRef,
      @PathVariable(name = "segmentName") String segmentName
  ) {
    log.info("Got call for getQueryForCustomerSegment for customerId: {}, botRef: {}, segmentName: {}", customerId, botRef, segmentName);
    try {
      CustomerSegmentResponse<List<CustomerSegmentationResponse>> response = generateSegmentsService.getCustomersForSegment(customerId, botRef, segmentName);
        return new ResponseEntity<>(response, response.getStatusCode());
    }catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }


}
