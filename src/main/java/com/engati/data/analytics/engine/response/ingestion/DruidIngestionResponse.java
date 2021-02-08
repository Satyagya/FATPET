package com.engati.data.analytics.engine.response.ingestion;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class DruidIngestionResponse {

  private Long customerId;

  private Long botRef;

  private String taskId;

}
