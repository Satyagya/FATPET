package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.service.SegmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;


@RestController
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@Slf4j
public class SegmentController {

  @Autowired
  private SegmentService segmentService;

  @GetMapping(value = ApiPathConstants.GET_CUSTOMER_SEGMENT)
  public ResponseEntity<DataAnalyticsResponse> getCustomerSegment(
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef,
      @PathVariable(name = ApiPathConstants.SEGMENT_NAME) String segmentName ) {
    log.info("Got call for getCustomerSegment for botRef: {}, segmentName: {}", botRef, segmentName);
      DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = segmentService.getCustomersForSegment(botRef, segmentName);
        return new ResponseEntity<>(response, response.getStatusCode());
  }

}
