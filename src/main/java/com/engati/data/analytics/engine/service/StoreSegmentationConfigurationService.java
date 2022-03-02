package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.model.SegmentConfigResponse;
import com.engati.data.analytics.engine.model.request.SegmentationConfigurationRequest;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;


public interface StoreSegmentationConfigurationService {

  SegmentConfigResponse<SegmentationConfigurationResponse> getConfigByBotRefAndSegment (Long customerId, Long botRef, String segmentName);

  SegmentConfigResponse<SegmentationConfigurationResponse> updateConfigByBotRefAndSegment(Long customerId, Long botRef, String segmentName, SegmentationConfigurationRequest segmentationConfigurationRequest);
}
