package com.personal.fatpet.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.personal.fatpet.constant.FatPetConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@ToString(doNotUseGetters = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = FatPetConstants.USER_PET_DETAILS)
public class UserPetDetails {
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Column(name = "USER_ID")
  private String userId;

  @Column(name = "PET_NAME")
  private String petName;

  @Column(name = "SPECIE")
  private String specie;

  @Column(name = "BREED")
  private String breed;

  @Column(name = "AGE")
  private String age;

  @Column(name = "ACTIVITY_LEVEL")
  private String activityLevel;

  @Column(name = "APPROX_SIZE")
  private String approxSize;
}
