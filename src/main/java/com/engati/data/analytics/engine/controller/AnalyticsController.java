package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.model.request.CustomerDetailsRequest;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.request.PurchaseHistoryRequest;
import com.engati.data.analytics.engine.model.response.CustomerDetailsResponse;
import com.engati.data.analytics.engine.model.response.OrderDetailsResponse;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;
import com.engati.data.analytics.engine.service.AnalyticsService;
import com.engati.data.analytics.engine.service.PrometheusManagementService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PathVariable;



import java.util.List;

@RestController
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@Slf4j
public class AnalyticsController {

  @Autowired
  private AnalyticsService analyticsService;

  @Autowired
  @Qualifier("com.engati.data.analytics.engine.service.impl.PrometheusManagementServiceImpl")
  private PrometheusManagementService prometheusManagementService;

  @PostMapping(value = ApiPathConstants.API_BASE_PATH_FOR_VARIANTS_BY_UNIT_SALES)
  public ResponseEntity<DataAnalyticsResponse<List<ProductVariantResponse>>> getVariantsByUnitSales(
      @RequestBody ProductDiscoveryRequest productDiscoveryRequest,
      @PathVariable(name = ApiPathConstants.BOTREF) Long botRef) {
    log.info("Got call for get variants by Unit Sales for botRef: {}, with requestBody {}", botRef, productDiscoveryRequest);
    DataAnalyticsResponse<List<ProductVariantResponse>> response = analyticsService.getVariantsByUnitSales(botRef, productDiscoveryRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.API_BASE_PATH_FOR_FETCHING_PURCHASE_HISTORY)
  public ResponseEntity<DataAnalyticsResponse<List<OrderDetailsResponse>>> getPurchaseHistory(
      @RequestBody PurchaseHistoryRequest purchaseHistoryRequest
  ){
    log.info("Got call for fetching purchase History for botRef: {}, requestbody: {}", purchaseHistoryRequest.getBotRef(), purchaseHistoryRequest);
    DataAnalyticsResponse<List<OrderDetailsResponse>> response = analyticsService.getPurchaseHistory(purchaseHistoryRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = ApiPathConstants.API_BASE_PATH_FOR_FETCHING_CUSTOMER_DETAILS)
  public ResponseEntity<DataAnalyticsResponse<CustomerDetailsResponse>> getCustomerDetails( @RequestBody
      CustomerDetailsRequest customerDetailsRequest, @PathVariable(name = ApiPathConstants.BOTREF) Long botRef){
    log.info("Got call for fetching customer details for botRef: {}, requestBody: {}", botRef, customerDetailsRequest);
    DataAnalyticsResponse<CustomerDetailsResponse> response = analyticsService.getCustomerDetails(botRef, customerDetailsRequest);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

}