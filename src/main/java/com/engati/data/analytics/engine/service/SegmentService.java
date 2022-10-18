package com.engati.data.analytics.engine.service;
import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.model.request.CustomSegmentRequest;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationCustomSegmentResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;

import java.util.List;


public interface SegmentService {

    DataAnalyticsResponse<List<CustomerSegmentationResponse>> getCustomersForSystemSegment(Long botRef, String segmentName);

    DataAnalyticsResponse<List<CustomerSegmentationCustomSegmentResponse>> getCustomersForCustomSegment(Long botRef, CustomSegmentRequest customSegmentRequest);

    DataAnalyticsResponse<List<String>> getProductType(Long botRef);
    DataAnalyticsResponse<List<String>> getCollection(Long botRef);
    DataAnalyticsResponse<List<String>> getCountry(Long botRef);

}
