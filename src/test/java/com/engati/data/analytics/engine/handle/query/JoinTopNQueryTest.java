package com.engati.data.analytics.engine.handle.query;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JoinTopNQueryTest {

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {
    Mockito.verifyNoMoreInteractions();
  }
}
