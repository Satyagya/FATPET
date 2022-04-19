package com.engati.data.analytics.engine.model.response;

import com.opencsv.bean.CsvBindByName;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(doNotUseGetters = true)
public class CustomerSegmentationResponse {

  @CsvBindByName(column = "CUSTOMER NAME")
  private String customerName;

  @CsvBindByName(column = "CUSTOMER EMAIL")
  private String customerEmail;

  @CsvBindByName(column = "CUSTOMER PHONE")
  private String customerPhone;

  @CsvBindByName(column = "STORE AOV")
  private Double storeAOV;

  @CsvBindByName(column = "CUSTOMER AOV")
  private Double customerAOV;

  @CsvBindByName(column = "ORDERS IN LAST ONE MONTH")
  private Integer ordersInLastOneMonth;

  @CsvBindByName(column = "ORDERS IN LAST SIX MONTH")
  private Integer ordersInLastSixMonths;

  @CsvBindByName(column = "ORDERS IN LAST TWELVE MONTH")
  private Integer ordersInLastTwelveMonths;

}