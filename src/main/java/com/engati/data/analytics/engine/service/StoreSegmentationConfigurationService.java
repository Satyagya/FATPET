package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.entity.StoreSegmentationConfiguration;
import com.engati.data.analytics.engine.model.request.SegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;

import javax.xml.crypto.Data;


public interface StoreSegmentationConfigurationService {

  DataAnalyticsResponse<SegmentationConfigurationResponse> getConfigByBotRefAndSegment (Long customerId, Long botRef, String segmentName);

  DataAnalyticsResponse<StoreSegmentationConfiguration> updateConfigByBotRefAndSegment(Long customerId, Long botRef, String segmentName, SegmentationConfigurationRequest segmentationConfigurationRequest);
}
