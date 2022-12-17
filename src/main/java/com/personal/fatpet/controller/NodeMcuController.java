package com.personal.fatpet.controller;

import com.personal.fatpet.entity.FatPetUserConfig;
import com.personal.fatpet.entity.UserDetailsConfig;
import com.personal.fatpet.model.FatPetResponse;
import com.personal.fatpet.service.NodeMcuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Slf4j
@RequestMapping("/v1")
@Controller
public class NodeMcuController {

  @Autowired
  private NodeMcuService nodeMcuService;

  @GetMapping(value = "/fatPetUser/initalConnection/")
  public ResponseEntity<FatPetResponse> createConnectionForUser(
      @RequestParam String userName, @RequestParam String ipAddress, @RequestParam String ssid) {
    log.info("Received new connection with Name: {}, ip: {}, ssid: {}", userName, ipAddress, ssid);
    FatPetResponse<UserDetailsConfig> response =
        nodeMcuService.saveInitialDetailsAndGetUserId(userName, ipAddress, ssid);
    return new ResponseEntity<>(response, response.getStatusCode());
  }

}
