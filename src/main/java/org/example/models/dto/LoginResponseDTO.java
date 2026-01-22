package org.example.models.dto;

public record LoginResponseDTO(
        String email,
        String name,
        String token
)

{
}
