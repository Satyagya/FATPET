package com.engati.data.analytics.engine.model.request;

import lombok.Data;

@Data
public class CustomSegmentRequest {
  private String segmentCondition;
  private String segmentName;
}
