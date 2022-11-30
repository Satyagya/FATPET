package com.engati.data.analytics.engine.model.response;


import lombok.Data;

@Data
public class CustomerDetailsResponse {

  private String customerName;
  private String customerEmail;
  private String customerPhone;
  private Double storeAOV;
  private Double customerAOV;
  private Long ordersInLastOneMonth;
  private Long ordersInLastSixMonths;
  private Long ordersInLastTwelveMonths;
  private String lastOrderDate;

}