package org.example.controllers;

import org.example.models.dto.LoginRequestDTO;
import org.example.models.dto.LoginResponseDTO;
import org.example.models.dto.RegistrationDTO;
import org.example.models.entity.ApplicationUser;
import org.example.service.AuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/auth")
@CrossOrigin("*")
public class AuthenticationController {

    private final AuthenticationService authenticationService;

    public AuthenticationController(AuthenticationService authenticationService) {
        this.authenticationService = authenticationService;
    }

    @PostMapping("/register")
    public ApplicationUser registerUser(@RequestBody RegistrationDTO body){
        return authenticationService.registerUser(body.name(), body.email(), body.password());
    }

    @PostMapping("/login")
    public LoginResponseDTO loginUser(@RequestBody LoginRequestDTO body){
        return authenticationService.loginUser(body.email(), body.password());
    }
}