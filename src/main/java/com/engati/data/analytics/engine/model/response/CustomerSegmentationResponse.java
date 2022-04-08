package com.engati.data.analytics.engine.model.response;

import com.opencsv.bean.CsvBindByName;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(doNotUseGetters = true)
public class CustomerSegmentationResponse {

  @CsvBindByName(column = "CUSTOMER_NAME")
  private String customerName;

  @CsvBindByName(column = "CUSTOMER_EMAIL")
  private String customerEmail;

  @CsvBindByName(column = "CUSTOMER_PHONE")
  private String customerPhone;

  @CsvBindByName(column = "STORE_AOV")
  private Double storeAOV;

  @CsvBindByName(column = "CUSTOMER_AOV")
  private Double customerAOV;

  @CsvBindByName(column = "ORDERS_IN_LAST_ONE_MONTH")
  private Integer ordersInLastOneMonth;

  @CsvBindByName(column = "ORDERS_IN_LAST_SIX_MONTH")
  private Integer ordersInLastSixMonths;

  @CsvBindByName(column = "ORDERS_IN_LAST_TWELVE_MONTH")
  private Integer ordersInLastTwelveMonths;

}