package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.entity.CustomerSegmentationConfiguration;
import com.engati.data.analytics.engine.model.request.CustomerSegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationConfigurationResponse;


public interface CustomerSegmentationConfigurationService {

  DataAnalyticsResponse<CustomerSegmentationConfigurationResponse> getConfigByBotRefAndSegment (Long botRef, String segmentName);


  DataAnalyticsResponse<CustomerSegmentationConfiguration> updateConfigByBotRefAndSegment(Long botRef, String segmentName, CustomerSegmentationConfigurationRequest customerSegmentationConfigurationRequest);
}
