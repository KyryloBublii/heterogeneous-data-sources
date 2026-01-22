package org.example.models.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.ColumnDefault;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

@Setter
@Getter
@Entity
@Table(name = "\"user\"", schema = "auth")
public class ApplicationUser implements UserDetails {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "user_id", nullable = false)
    private Long id;

    @Column(name = "user_uid", nullable = false, length = 40)
    private String userUid;

    @NotBlank(message = "Name is mandatory")
    @Column(name = "name", nullable = false, length = 60)
    private String name;

    @Email(message = "Email must be valid")
    @NotBlank(message = "Email is mandatory")
    @Column(name = "email", nullable = false, length = 60, unique = true)
    private String email;

    @Column(name = "password_hash", nullable = false, length = Integer.MAX_VALUE)
    private String passwordHash;

    @ColumnDefault("now()")
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    public ApplicationUser(String id, String name, String email, String encoded) {
        this.userUid = id;
        this.name = name;
        this.email = email;
        this.passwordHash = encoded;
        this.createdAt = Instant.now();
    }

    public ApplicationUser() {
    }



    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return List.of();
    }

    @Override
    public String getPassword() {
        return passwordHash;
    }

    @Override
    public String getUsername() {
        return email;
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}