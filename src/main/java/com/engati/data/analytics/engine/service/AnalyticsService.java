package com.engati.data.analytics.engine.service;

import com.engati.data.analytics.engine.common.model.DataAnalyticsResponse;
import com.engati.data.analytics.engine.model.request.ProductDiscoveryRequest;
import com.engati.data.analytics.engine.model.response.ProductVariantResponse;

import java.util.List;

public interface AnalyticsService {

   DataAnalyticsResponse<List<ProductVariantResponse>> getVariantsByUnitSales(Long customerId, Long botRef, ProductDiscoveryRequest productDiscoveryRequest);
}
