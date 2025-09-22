package com.lineage.controller;

import com.lineage.dto.ConfigDtos.ConfigCreateRequest;
import com.lineage.dto.ConfigDtos.ConfigDto;
import com.lineage.dto.ConfigDtos.ConfigUpdateRequest;
import com.lineage.entity.Config;
import com.lineage.repository.ConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;

@RestController
@RequestMapping("/config")
@CrossOrigin(origins = "*")
public class ConfigController {

    private static final Logger logger = LoggerFactory.getLogger(ConfigController.class);

    @Autowired private ConfigRepository configRepository;

    @GetMapping("")
    public ResponseEntity<List<ConfigDto>> list() {
        List<Config> all = configRepository.findAll();
        return ResponseEntity.ok(toDtos(all));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ConfigDto> get(@PathVariable UUID id) {
        return configRepository.findById(id)
                .map(c -> ResponseEntity.ok(toDto(c)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("")
    public ResponseEntity<ConfigDto> create(@RequestBody ConfigCreateRequest req) {
        Config c = new Config();
        c.setProfile(req.getProfile());
        c.setRepositoryUrl(req.getRepositoryUrl());
        c.setBranch(req.getBranch());
        c.setGithubToken(req.getGithubToken());
        c.setGroqKey(req.getGroqKey());
        c = configRepository.save(c);
        return ResponseEntity.ok(toDto(c));
    }

    @PatchMapping("/{id}")
    public ResponseEntity<ConfigDto> update(@PathVariable UUID id, @RequestBody ConfigUpdateRequest req) {
        Optional<Config> maybe = configRepository.findById(id);
        if (maybe.isEmpty()) return ResponseEntity.notFound().build();
        Config c = maybe.get();
        if (req.getProfile() != null) c.setProfile(req.getProfile());
        if (req.getRepositoryUrl() != null) c.setRepositoryUrl(req.getRepositoryUrl());
        if (req.getBranch() != null) c.setBranch(req.getBranch());
        if (req.getGithubToken() != null) c.setGithubToken(req.getGithubToken());
        if (req.getGroqKey() != null) c.setGroqKey(req.getGroqKey());
        c = configRepository.save(c);
        return ResponseEntity.ok(toDto(c));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        configRepository.deleteById(id);
        return ResponseEntity.ok().build();
    }

    private List<ConfigDto> toDtos(List<Config> configs) {
        List<ConfigDto> out = new ArrayList<>();
        for (Config c : configs) out.add(toDto(c));
        return out;
    }

    private ConfigDto toDto(Config c) {
        ConfigDto dto = new ConfigDto();
        dto.setId(c.getId());
        dto.setProfile(c.getProfile());
        dto.setRepositoryUrl(c.getRepositoryUrl());
        dto.setBranch(c.getBranch());
        dto.setGithubTokenMasked(mask(c.getGithubToken()));
        dto.setGroqKeyMasked(mask(c.getGroqKey()));
        dto.setCreatedAt(c.getCreatedAt());
        dto.setUpdatedAt(c.getUpdatedAt());
        return dto;
    }

    private String mask(String raw) {
        if (raw == null || raw.length() < 6) return "***";
        return raw.substring(0, 3) + "***" + raw.substring(raw.length()-3);
    }
}


