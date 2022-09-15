package com.engati.data.analytics.engine.model.response;

import com.opencsv.bean.CsvBindByName;
import lombok.*;

import java.sql.Date;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString(doNotUseGetters = true)

public class CustomerSegmentationCustomSegmentResponse implements Comparable<CustomerSegmentationCustomSegmentResponse>{
    @CsvBindByName(column = "NAME")
    private String customerName;

    @CsvBindByName(column = "EMAIL")
    private String customerEmail;

    @CsvBindByName(column = "PHONE")
    private String customerPhone;

    @CsvBindByName(column = "STORE AOV")
    private Double storeAOV;

    @CsvBindByName(column = "ORDERS")
    private Long customerOrders;

    @CsvBindByName(column = "AVERAGE ORDER VALUE")
    private Double customerAOV;

    @CsvBindByName(column = "REVENUE")
    private Double customerRevenue;

    @CsvBindByName(column = "LAST_ORDER_DATE")
    private Date customerLastOrderDate;

    @CsvBindByName(column = "PRODUCT TYPES")
    private String customerProductTypes;

    @Override
    public int compareTo(CustomerSegmentationCustomSegmentResponse customerSegmentationCustomSegmentResponse) {
        return this.getCustomerAOV().compareTo(customerSegmentationCustomSegmentResponse.getCustomerAOV());
    }

}
