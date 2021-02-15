package com.engati.data.analytics.engine.druid.query.service;

import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import in.zapr.druid.druidry.filter.DruidFilter;

public interface DruidFilterGenerator {

  DruidFilter getFiltersByType(DruidFilterMetaInfo druidFilterMetaInfoDto,
      Integer botRef, Integer customerId);
}
