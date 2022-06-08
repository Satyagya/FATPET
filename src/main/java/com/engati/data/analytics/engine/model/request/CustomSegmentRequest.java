package com.engati.data.analytics.engine.model.request;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CustomSegmentRequest {
  private String segmentCondition;
  private String segmentName;

}
