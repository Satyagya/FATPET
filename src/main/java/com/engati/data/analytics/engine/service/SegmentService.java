package com.engati.data.analytics.engine.service;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.model.request.CustomSegmentRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;

import java.util.List;
import java.util.Map;


public interface SegmentService {

    DataAnalyticsResponse<List<CustomerSegmentationResponse>> getCustomersForSystemSegment(Long botRef, String segmentName);

    Map<String, Object> getCustomersForCustomSegment(Long botRef, CustomSegmentRequest customSegmentRequest);


}
