package org.example.models.dto;

import com.fasterxml.jackson.annotation.JsonAlias;

public record LoginRequestDTO(
        @JsonAlias({"username"}) String email,
        String password
) {
}