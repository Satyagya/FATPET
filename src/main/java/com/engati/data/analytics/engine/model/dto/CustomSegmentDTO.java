package com.engati.data.analytics.engine.model.dto;

import com.engati.data.analytics.engine.constants.constant.KafkaConstants;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomSegmentDTO {

  @JsonProperty(value = KafkaConstants.BOT_REF)
  private Integer botRef;

  @JsonProperty(value = KafkaConstants.SEGMENT_CONDITION)
  private String segmentCondition;

  @JsonProperty(value = KafkaConstants.SEGMENT_NAME)
  private String segmentName;

  @JsonProperty(value = KafkaConstants.SEGMENT_TYPE)
  private String segmentType;

}