package com.engati.data.analytics.engine.model.response;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(doNotUseGetters = true)
public class CustomerSegmentationResponse {

  private String customerName;
  private String customerEmail;
  private String customerPhone;
  private Double storeAOV;
  private Double customerAOV;
  private Long ordersInLastOneMonth;
  private Long ordersInLastSixMonths;
  private Long ordersInLastTwelveMonths;

}