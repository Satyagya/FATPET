package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;
import com.engati.data.analytics.engine.service.AnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PathVariable;



import java.util.List;

@RestController
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@Slf4j
public class AnalyticsController {

  @Autowired
  private AnalyticsService analyticsService;

  @GetMapping(value = ApiPathConstants.API_BASE_PATH_FOR_VARIANTS_BY_UNIT_SALES)
  public ResponseEntity<DataAnalyticsResponse<List<ProductVariantResponse>>> getVariantsByUnitSales(
      @RequestBody ProductDiscoveryRequest productDiscoveryRequest,
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef,
      @PathVariable(name = ApiPathConstants.CUSTOMERID) Long customerId) {
    log.info("Got call for get variants by Unit Sales for customerId: {}, botRef: {}, segmentName: {}", customerId, botRef);
    DataAnalyticsResponse<List<ProductVariantResponse>> response = analyticsService.getVariantsByUnitSales(customerId, botRef, productDiscoveryRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }
}