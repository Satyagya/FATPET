package com.engati.data.analytics.engine.entity;

import com.engati.data.analytics.engine.constants.constant.ApiPathConstants;
import com.engati.data.analytics.engine.constants.constant.TableConstants;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString(doNotUseGetters = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = ApiPathConstants.SHOPIFY_GOOGLE_ANALYTICS_INFO)
public class ShopifyGoogleAnalyticsInfo {

  @Id
  @Column(name = TableConstants.BOT_REF)
  private Integer botRef;

  @Column(name = TableConstants.CREDENTIALS)
  private String credentials;

  @Column(name = TableConstants.PROPERTY_ID)
  private Integer propertyId;

  @Column(name = TableConstants.CREATED_AT)
  private Timestamp createdAt;

}
