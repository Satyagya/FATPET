package com.engati.data.analytics.engine.model.response;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(doNotUseGetters = true)
public class CustomerSegmentationConfigurationResponse {

  private Long botRef;
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
