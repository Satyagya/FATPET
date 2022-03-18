package com.engati.data.analytics.engine.service;
import com.engati.data.analytics.engine.common.model.CustomerSegmentResponse;
import com.engati.data.analytics.engine.model.response.CustomerSegmentationResponse;

import java.sql.SQLException;
import java.util.List;


public interface GenerateSegmentsService {
    CustomerSegmentResponse<List<CustomerSegmentationResponse>> getCustomersForSegment(Long customerId, Long botRef, String segmentName) throws SQLException;

}
