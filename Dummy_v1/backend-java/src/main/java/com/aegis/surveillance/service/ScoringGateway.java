package com.aegis.surveillance.service;
import com.aegis.surveillance.model.Assessment;
import com.aegis.surveillance.model.ScoreRequest;
import com.aegis.surveillance.model.InvestigationCopilotRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
@Service
public class ScoringGateway {
  private final RestClient client;
  public ScoringGateway(@Value("${aegis.ml-base-url}") String baseUrl) {
    this.client = RestClient.builder()
        .baseUrl(baseUrl)
        .requestFactory(requestFactory())
        .build();
  }
  private SimpleClientHttpRequestFactory requestFactory() {
    var factory = new SimpleClientHttpRequestFactory();
    factory.setConnectTimeout(3000);
    factory.setReadTimeout(15000);
    return factory;
  }
  public Assessment score(ScoreRequest request) {
    return client.post().uri("/v1/score").contentType(MediaType.APPLICATION_JSON).body(request).retrieve().body(Assessment.class);
  }
  @SuppressWarnings("unchecked")
  public Map<String, Object> fileJobs() {
    return client.get().uri("/v1/file-jobs").retrieve().body(Map.class);
  }
  @SuppressWarnings("unchecked")
  public Map<String, Object> scanFileJobs() {
    return client.post().uri("/v1/file-jobs/scan").retrieve().body(Map.class);
  }
  @SuppressWarnings("unchecked")
  public Map<String, Object> modelCard() {
    return client.get().uri("/v1/model-card").retrieve().body(Map.class);
  }
  @SuppressWarnings("unchecked")
  public Map<String, Object> dailyCases() {
    return client.get().uri("/v1/daily-batches/cases?limit=250").retrieve().body(Map.class);
  }
  @SuppressWarnings("unchecked")
  public Map<String, Object> investigationCopilot(InvestigationCopilotRequest request) {
    try {
      return client.post().uri("/v1/investigation-copilot").contentType(MediaType.APPLICATION_JSON)
          .body(request).retrieve().body(Map.class);
    } catch (RestClientResponseException exception) {
      // Preserve the controlled status but never echo provider or evidence content through Java.
      throw new ResponseStatusException(exception.getStatusCode(),
          "Investigation copilot request was safely rejected by the ML service");
    }
  }
}
