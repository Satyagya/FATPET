package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.model.request.CustomSegmentRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationCustomSegmentResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;
import com.engati.data.analytics.engine.service.SegmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@Slf4j
public class SegmentController {

  @Autowired
  private SegmentService segmentService;

  @GetMapping(value = ApiPathConstants.GET_SYSTEM_SEGMENT)
  public ResponseEntity<DataAnalyticsResponse> getSystemSegment(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @PathVariable(name = ApiPathConstants.SEGMENT_NAME) String segmentName) {
    log.info("Got call for getCustomerSegment for botRef: {}, segmentName: {}", botRef, segmentName);
    DataAnalyticsResponse<List<CustomerSegmentationResponse>> response = segmentService.getCustomersForSystemSegment(botRef, segmentName);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @GetMapping(value = ApiPathConstants.GET_CUSTOM_SEGMENT)
  public ResponseEntity<DataAnalyticsResponse> getCustomSegment(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef, @RequestBody CustomSegmentRequest customSegmentRequest) {
      log.info("Got call for getCustomerSegment for botRef: {}, customSegmentRequest: {}", botRef, customSegmentRequest);
      DataAnalyticsResponse<List<CustomerSegmentationCustomSegmentResponse>> response = segmentService.getCustomersForCustomSegment(botRef, customSegmentRequest);
      return new ResponseEntity<>(response, response.getStatusCode());
  }

  @GetMapping(value = ApiPathConstants.GET_PRODUCT_TYPE)
  public ResponseEntity<DataAnalyticsResponse> getProductType(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef) {
    log.info("Got call to fetch productType for botRef: {}", botRef);
    DataAnalyticsResponse<List<String>> response = segmentService.getProductType(botRef);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @GetMapping(value = ApiPathConstants.GET_COLLECTION)
  public ResponseEntity<DataAnalyticsResponse> getCollection(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef) {
    log.info("Got call to fetch collection for botRef:{}", botRef);
    DataAnalyticsResponse<List<String>> response = segmentService.getCollection(botRef);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @GetMapping(value = ApiPathConstants.GET_COUNTRY)
  public ResponseEntity<DataAnalyticsResponse> getCountry(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef) {
    log.info("Got call to fetch country for botRef:{}", botRef);
    DataAnalyticsResponse<List<String>> response = segmentService.getCountry(botRef);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @GetMapping(value = ApiPathConstants.GET_CITY)
  public ResponseEntity<DataAnalyticsResponse> getCity(@PathVariable(name = ApiPathConstants.BOTREF) Long botRef) {
    log.info("Got call to fetch country for botRef:{}", botRef);
    DataAnalyticsResponse<List<String>> response = segmentService.getCity(botRef);
    return new ResponseEntity<>(response, response.getStatusCode());
  }
}
