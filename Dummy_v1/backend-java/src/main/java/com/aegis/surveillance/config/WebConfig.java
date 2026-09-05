package com.aegis.surveillance.config;

import java.util.Arrays;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
  private final String[] origins;

  public WebConfig(@Value("${aegis.cors-origins}") String origins) {
    this.origins = Arrays.stream(origins.split(",")).map(String::trim).toArray(String[]::new);
  }

  @Override
  public void addCorsMappings(CorsRegistry registry) {
    registry.addMapping("/v1/**").allowedOrigins(origins)
        .allowedMethods("GET", "POST", "OPTIONS").allowedHeaders("Content-Type", "X-Aegis-Key");
  }
}
