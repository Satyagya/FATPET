package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.ShopifyCity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Set;

public interface ShopifyCityRepository extends JpaRepository<ShopifyCity,Long> {

    @Query (value = "select city from shopify_city where country in :countries",nativeQuery = true)
    List<String> getAllCities(@Param("countries") List<String> countries);
}
