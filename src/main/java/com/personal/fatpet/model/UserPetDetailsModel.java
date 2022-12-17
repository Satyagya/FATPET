package com.personal.fatpet.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.personal.fatpet.service.NodeMcuService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Autowired;

import javax.persistence.Column;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@ToString(doNotUseGetters = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true, value = {"scheduledBroadcastModel"}, allowGetters = true)

public class UserPetDetailsModel {

  private String petName;

  private String specie;

  private String breed;

  private int age;

  private int activityLevel;

  private int weight;

  private int height;

  private String foodName;
}
