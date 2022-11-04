package com.engati.data.analytics.engine.entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "shopify_city")
public class ShopifyCity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "city")
    private String city;

    @Column(name = "country")
    private String country;

}
