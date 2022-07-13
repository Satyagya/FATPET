package com.engati.data.analytics.engine.controller;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.service.ShopifyGoogleAnalyticsService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping(ApiPathConstants.API_BASE_PATH)
@Slf4j
public class ShopifyGoogleAnalyticsController {

  @Autowired
  private ShopifyGoogleAnalyticsService shopifyGoogleAnalyticsService;

  @PostMapping(ApiPathConstants.SAVE_GA_CREDS)
  public ResponseEntity<DataAnalyticsResponse<String>> uploadDeleteGACreds(
      @RequestParam(ApiPathConstants.AUTH_JSON) MultipartFile authJson,
      @RequestParam(ApiPathConstants.BOTREF) @NonNull Integer botRef,
      @RequestParam(ApiPathConstants.PROPERTY_ID) Integer propertyId) {
    log.info("Received request for updating ga creds for botref {}", botRef);
    return new ResponseEntity<>(
        shopifyGoogleAnalyticsService.uploadDeleteGACreds(authJson, botRef, propertyId),
        HttpStatus.OK);
  }

}
