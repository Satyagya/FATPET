package com.personal.fatpet.controller;

import com.personal.fatpet.constant.FatPetConstants;
import com.personal.fatpet.entity.FatPetUserConfig;
import com.personal.fatpet.model.FatPetResponse;
import com.personal.fatpet.service.NodeMcuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController("com.personal.fatPet.controller.FatPetAppController")
@RequestMapping(value = FatPetConstants.API_BASE_PATH_V1)
public class FatPetAppController {

  @Autowired
  private NodeMcuService nodeMcuService;

  @GetMapping(value = "/fatPetUser/{user_id}/schedule")
  public ResponseEntity<FatPetResponse<FatPetUserConfig>> enterSchedulerDataForUser(
      @RequestParam(FatPetConstants.USER_ID) String userId) {
    log.info(
        "Make entry for user");
    FatPetResponse<FatPetUserConfig> response = null;
    return new ResponseEntity<>(response, response.getStatusCode());
  }

}
