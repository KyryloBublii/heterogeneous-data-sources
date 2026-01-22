package org.example.models.dto;

import com.fasterxml.jackson.annotation.JsonAlias;

import java.util.Objects;

public record RegistrationDTO(
        String name,
        @JsonAlias({"username"}) String email,
        String password
) {

    public RegistrationDTO {
        email = Objects.requireNonNullElse(email, "").trim();
        if (email.isEmpty()) {
            throw new IllegalArgumentException("Email is required");
        }

        if (name == null || name.isBlank()) {
            name = email;
        }
    }
}