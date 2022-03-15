package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.model.SegmentConfigResponse;
import com.engati.data.analytics.engine.model.response.SegmentationConfigurationResponse;

import java.sql.SQLException;
import java.util.Set;

public interface GenerateSegmentsService {

  Set<Long> getCustomersForSegment(Long customerId, Long botRef, String segmentName) throws SQLException;
}
