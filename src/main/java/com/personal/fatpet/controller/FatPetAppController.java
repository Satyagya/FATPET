package com.personal.fatpet.controller;

import com.personal.fatpet.constant.FatPetConstants;
import com.personal.fatpet.entity.FatPetUserConfig;
import com.personal.fatpet.model.FatPetResponse;
import com.personal.fatpet.model.UserPetDetailsModel;
import com.personal.fatpet.service.NodeMcuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotBlank;
import java.util.List;

@Slf4j
@RestController("com.personal.fatPet.controller.FatPetAppController")
@RequestMapping(value = FatPetConstants.API_BASE_PATH_V1)
public class FatPetAppController {

  @Autowired
  private NodeMcuService nodeMcuService;

  @GetMapping(value = "/fatPetUser/{userId}/petDetails")
  public ResponseEntity<FatPetResponse> setPetDetails( @RequestParam @NotBlank String userId,
      @RequestBody UserPetDetailsModel userPetDetailsModel) {
    log.info("Got request to set Pet details for user: {}, model: {}", userId, userPetDetailsModel);
    FatPetResponse<Boolean> response =
        nodeMcuService.savePetDetails(userId, userPetDetailsModel);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @PostMapping(value = "/fatPetUser/{userId}/getRecommendedSettings")
  public ResponseEntity<FatPetResponse<List<FatPetUserConfig>>> getRecommendedSettings(
      @RequestParam @NotBlank String userId) {
    log.info("Got request to get Recommended Settings for user: {}", userId);
    FatPetResponse<List<FatPetUserConfig>> response = nodeMcuService.getRecommendedSettings(userId);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

  @GetMapping(value = "/fatPetUser/{user_id}/schedule")
  public ResponseEntity<FatPetResponse<FatPetUserConfig>> enterSchedulerDataForUser(
      @RequestParam(FatPetConstants.USER_ID) String userId) {
    log.info(
        "Make entry for user");
    FatPetResponse<FatPetUserConfig> response = null;
    return new ResponseEntity<>(response, response.getStatusCode());
  }

}
