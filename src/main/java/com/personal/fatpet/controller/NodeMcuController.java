package com.personal.fatpet.controller;

import com.personal.fatpet.service.NodeMcuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

  @GetMapping(value = "/fatPetUser/userConnection/{ipAddress}")
  public void createConnectionForUser(@RequestParam String ipAddress) {
    log.info("Make entry for user, {}", ipAddress);
  }

}
