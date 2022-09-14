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
    @CsvBindByName(column = "CUSTOMER NAME")
    private String customerName;

    @CsvBindByName(column = "CUSTOMER EMAIL")
    private String customerEmail;

    @CsvBindByName(column = "CUSTOMER PHONE")
    private String customerPhone;

    @CsvBindByName(column = "STORE AOV")
    private Double storeAOV;

    @CsvBindByName(column = "CUSTOMER ORDERS")
    private Long customerOrders;

    @CsvBindByName(column = "CUSTOMER AOV")
    private Double customerAOV;

    @CsvBindByName(column = "CUSTOMER REVENUE")
    private Double customerRevenue;

    @CsvBindByName(column = "CUSTOMER LAST_ORDER_DATE")
    private Date customerLastOrderDate;

    @CsvBindByName(column = "PRODUCT TYPES")
    private String productTypes;

    @Override
    public int compareTo(CustomerSegmentationCustomSegmentResponse customerSegmentationCustomSegmentResponse) {
        return this.getCustomerAOV().compareTo(customerSegmentationCustomSegmentResponse.getCustomerAOV());
    }

}
