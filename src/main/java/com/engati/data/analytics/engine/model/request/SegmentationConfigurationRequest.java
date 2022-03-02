package com.engati.data.analytics.engine.model.request;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(doNotUseGetters = true)
public class SegmentationConfigurationRequest {

  private Long botRef;
  private Long customerId;
  private String segmentName;
  private String recencyMetric;
  private String recencyOperator;
  private Long recencyValue;
  private Long frequencyMetric;
  private String frequencyOperator;
  private Long frequencyValue;
  private String monetaryMetric;
  private String monetaryOperator;
  private String monetaryValue;

}
