package com.personal.fatpet.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.http.HttpStatus;

@Getter
@Setter
@ToString(doNotUseGetters = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FatPetResponse<T> {
  FatPetStatusCode status;

  T responseObject;

  @JsonIgnore
  private HttpStatus statusCode;

  public FatPetResponse() {
    this.statusCode = HttpStatus.OK;
  }

  public FatPetResponse(FatPetStatusCode status) {
    this();
    this.status = status;
  }

  public FatPetResponse(T responseObject, FatPetStatusCode status) {
    this();
    this.responseObject = responseObject;
    this.status = status;
  }
}