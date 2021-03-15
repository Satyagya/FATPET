package com.engati.data.analytics.engine.druid.query;

import com.engati.data.analytics.engine.druid.query.service.impl.DruidPostAggregateGeneratorImpl;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorMetaInfo;
import com.engati.data.analytics.sdk.druid.postaggregator.DruidPostAggregatorType;
import in.zapr.druid.druidry.postAggregator.DruidPostAggregator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DruidPostAggregatorImplTest {

  @InjectMocks
  private DruidPostAggregateGeneratorImpl druidPostAggregateGenerator;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void druidPostAggregator_success() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    List<DruidPostAggregatorMetaInfo> postAggregatorMetaInfo = new ArrayList<>();
    List<DruidPostAggregatorMetaInfo> fields = new ArrayList<>();
    fields.add(DruidPostAggregatorMetaInfo.builder()
        .type(DruidPostAggregatorType.FIELD_ACCESSOR).postAggregatorName("sales")
        .fieldName("sales").build());
    fields.add(DruidPostAggregatorMetaInfo.builder()
        .type(DruidPostAggregatorType.FINALIZING_FIELD_ACCESS)
        .postAggregatorName("order_count").fieldName("order_count").build());
    postAggregatorMetaInfo.add(DruidPostAggregatorMetaInfo.builder()
        .postAggregatorName("AOV").type(DruidPostAggregatorType.ARITHMETIC)
        .fields(fields).function("DIVIDE").build());
    List<DruidPostAggregator> actualResponse = druidPostAggregateGenerator
        .getQueryPostAggregators(postAggregatorMetaInfo, botRef, customerId);
    Assert.assertEquals(actualResponse.size(), 1);
    Assert.assertEquals(actualResponse.get(0).getType(), "arithmetic");
    Assert.assertEquals(actualResponse.get(0).getName(), "AOV");
  }

  @Test
  public void druidPostAggregator_null() {
    Integer botRef = 24075;
    Integer customerId = 5234;
    List<DruidPostAggregatorMetaInfo> postAggregatorMetaInfo = null;
    List<DruidPostAggregator> actualResponse = druidPostAggregateGenerator
        .getQueryPostAggregators(postAggregatorMetaInfo, botRef, customerId);
    Assert.assertEquals(actualResponse, Collections.emptyList());
  }
}
