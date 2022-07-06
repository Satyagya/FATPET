package com.engati.data.analytics.engine.repository;

import com.engati.data.analytics.engine.entity.ShopifyGoogleAnalyticsInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository("com.engati.broadcast.repository.ShopifyGoogleAnalyticsInfoRepository")
public interface ShopifyGoogleAnalyticsInfoRepository
    extends JpaRepository<ShopifyGoogleAnalyticsInfo, Integer> {
}
