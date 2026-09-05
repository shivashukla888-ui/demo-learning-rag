package com.aegis.surveillance.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiAccessFilter extends OncePerRequestFilter {
  private final String apiKey;
  private final String actorId;
  private final String actorRole;

  public ApiAccessFilter(@Value("${aegis.api-key}") String apiKey,
                         @Value("${aegis.actor-id}") String actorId,
                         @Value("${aegis.actor-role}") String actorRole) {
    this.apiKey = apiKey;
    this.actorId = actorId;
    this.actorRole = actorRole;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return request.getMethod().equals("OPTIONS")
        || request.getRequestURI().startsWith("/actuator/")
        || request.getRequestURI().equals("/error");
  }

  @Override
  protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    if (!apiKey.isBlank() && !apiKey.equals(request.getHeader("X-Aegis-Key"))) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Valid X-Aegis-Key is required");
      return;
    }
    request.setAttribute("aegis.actorId", actorId);
    request.setAttribute("aegis.actorRole", actorRole);
    response.setHeader("X-Content-Type-Options", "nosniff");
    response.setHeader("Cache-Control", "no-store");
    chain.doFilter(request, response);
  }
}
