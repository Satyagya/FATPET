package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;
import com.engati.data.analytics.engine.service.AnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1")
@Slf4j
public class AnalyticsController {

  @Autowired
  private AnalyticsService analyticsService;

  @GetMapping(value = "/customerId/{customerId}/botRef/{botRef}/getVariantsByUnitSales")
  public ResponseEntity<DataAnalyticsResponse<List<ProductVariantResponse>>> getVariantsByUnitSales(
      @RequestBody ProductDiscoveryRequest productDiscoveryRequest,
      @PathVariable(name = "botRef") Long botRef,
      @PathVariable(name = "customerId") Long customerId) {
    log.info("Got call for get variants by Unit Sales for customerId: {}, botRef: {}, segmentName: {}", customerId, botRef);
    DataAnalyticsResponse<List<ProductVariantResponse>> response = analyticsService.getVariantsByUnitSales(customerId, botRef, productDiscoveryRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }
}