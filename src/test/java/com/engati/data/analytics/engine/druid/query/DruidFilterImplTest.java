package com.engati.data.analytics.engine.druid.query;

import com.engati.data.analytics.engine.druid.query.service.impl.DruidFilterGeneratorImpl;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterMetaInfo;
import com.engati.data.analytics.sdk.druid.filters.DruidFilterType;
import in.zapr.druid.druidry.filter.DruidFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

public class DruidFilterImplTest {

  @InjectMocks
  private DruidFilterGeneratorImpl druidFilterGenerator;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void druidFilter_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    DruidFilterMetaInfo filterMetaInfo = DruidFilterMetaInfo.builder()
        .type(DruidFilterType.IN).dimension("city").value(Arrays.asList("chennai", "mumbai"))
        .build();
    DruidFilter actualResponse = druidFilterGenerator
        .getFiltersByType(filterMetaInfo, botRef, customerId);
    Assert.assertEquals(actualResponse.getType(), "in");
  }

  @Test
  public void druidFilter_null() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    DruidFilterMetaInfo filterMetaInfo = null;
    DruidFilter actualResponse = druidFilterGenerator
        .getFiltersByType(filterMetaInfo, botRef, customerId);
    Assert.assertNull(actualResponse);
  }
}
