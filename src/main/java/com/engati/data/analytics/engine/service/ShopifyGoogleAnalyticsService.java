package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import org.springframework.web.multipart.MultipartFile;

public interface ShopifyGoogleAnalyticsService {

  /**
   * service method to store ga creds into db
   *
   * @param authJson
   * @param botRef
   * @param propertyId
   * @return DataAnalyticsResponse
   */
  DataAnalyticsResponse<String> manageGACreds(MultipartFile authJson, Integer botRef,
      Integer propertyId);
}
