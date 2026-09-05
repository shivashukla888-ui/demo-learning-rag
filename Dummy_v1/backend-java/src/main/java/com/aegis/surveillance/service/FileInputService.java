package com.aegis.surveillance.service;

import com.aegis.surveillance.config.FileInputProperties;
import com.aegis.surveillance.model.AcceptedFileEvent;
import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@Service
public class FileInputService {
  private static final List<String> ZONES = List.of("inbound", "accepted", "rejected");
  private final FileInputProperties properties;
  private final AcceptedFileEventPublisher events;

  public FileInputService(FileInputProperties properties) {
    this(properties, new NoopAcceptedFileEventPublisher());
  }

  @Autowired
  public FileInputService(FileInputProperties properties, AcceptedFileEventPublisher events) {
    this.properties = properties;
    this.events = events;
  }

  @PostConstruct
  public void initialise() {
    if (!properties.isCreateDirectories()) return;
    try {
      for (String zone : ZONES) {
        for (String dataset : properties.getDatasets().keySet()) Files.createDirectories(directory(zone, dataset));
      }
    } catch (IOException exception) {
      throw new IllegalStateException("Could not initialise the configured file-input directories", exception);
    }
  }

  public Map<String, Object> configuration() {
    Map<String, Object> datasets = new LinkedHashMap<>();
    properties.getDatasets().forEach((key, value) -> datasets.put(key, Map.of(
        "required", value.isRequired(),
        "filenamePattern", value.getFilenamePattern(),
        "bootstrapFilename", value.getBootstrapFilename(),
        "requiredColumns", value.getRequiredColumns(),
        "inboundDirectory", directory("inbound", key).toString())));
    return Map.of(
        "rootDirectory", properties.getRootDirectory().toAbsolutePath().normalize().toString(),
        "sampleDirectory", properties.getSampleDirectory().toAbsolutePath().normalize().toString(),
        "bootstrapSamples", properties.isBootstrapSamples(),
        "maxFileSizeBytes", properties.getMaxFileSizeBytes(),
        "zones", ZONES,
        "datasets", datasets);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void bootstrapSamples() {
    if (!properties.isBootstrapSamples() || !Files.isDirectory(properties.getSampleDirectory())) return;
    for (var entry : properties.getDatasets().entrySet()) {
      Path acceptedDirectory = directory("accepted", entry.getKey());
      Path inboundDirectory = directory("inbound", entry.getKey());
      String bootstrapFilename = entry.getValue().getBootstrapFilename();
      boolean alreadyPresent = bootstrapFilename.isBlank()
          ? countFiles(acceptedDirectory) > 0 || countFiles(inboundDirectory) > 0
          : containsOriginalFile(acceptedDirectory, bootstrapFilename) || containsOriginalFile(inboundDirectory, bootstrapFilename);
      if (alreadyPresent) continue;
      try (Stream<Path> samples = Files.list(properties.getSampleDirectory())) {
        samples.filter(Files::isRegularFile)
            .filter(path -> matches(entry.getValue().getFilenamePattern(), path.getFileName().toString()))
            .filter(path -> entry.getValue().getBootstrapFilename().isBlank()
                || path.getFileName().toString().equals(entry.getValue().getBootstrapFilename()))
            .findFirst()
            .ifPresent(path -> importSample(entry.getKey(), path));
      } catch (IOException exception) {
        throw new IllegalStateException("Could not inspect the configured sample directory", exception);
      }
    }
  }

  public Map<String, Object> readiness() {
    var status = new LinkedHashMap<String, Object>();
    boolean ready = true;
    for (var entry : properties.getDatasets().entrySet()) {
      long pending = countFiles(directory("inbound", entry.getKey()));
      long accepted = countFiles(directory("accepted", entry.getKey()));
      boolean available = accepted > 0;
      if (entry.getValue().isRequired() && !available) ready = false;
      status.put(entry.getKey(), Map.of("required", entry.getValue().isRequired(), "available", available,
          "acceptedFiles", accepted, "pendingFiles", pending));
    }
    return Map.of("ready", ready, "checkedAt", Instant.now().toString(), "datasets", status);
  }

  public Map<String, Object> upload(String dataset, MultipartFile file) {
    var definition = requireDataset(dataset);
    if (file == null || file.isEmpty()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File is required");
    if (file.getSize() > properties.getMaxFileSizeBytes()) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "File exceeds configured size limit");
    String safeName = safeFilename(file.getOriginalFilename());
    if (!matches(definition.getFilenamePattern(), safeName)) {
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Filename must match " + definition.getFilenamePattern());
    }
    Path staged = directory("inbound", dataset).resolve(uniqueName(safeName));
    try {
      Files.copy(file.getInputStream(), staged, StandardCopyOption.REPLACE_EXISTING);
      return validateAndRoute(dataset, staged);
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not store input file", exception);
    }
  }

  public List<Map<String, Object>> scanPending() {
    var results = new ArrayList<Map<String, Object>>();
    for (String dataset : properties.getDatasets().keySet()) {
      try (Stream<Path> files = Files.list(directory("inbound", dataset))) {
        files.filter(Files::isRegularFile).sorted().forEach(file -> results.add(validateAndRoute(dataset, file)));
      } catch (IOException exception) {
        throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not scan inbound files", exception);
      }
    }
    return results;
  }

  private Map<String, Object> validateAndRoute(String dataset, Path staged) {
    var definition = requireDataset(dataset);
    List<String> errors = new ArrayList<>();
    String originalName = staged.getFileName().toString().replaceFirst("^[0-9a-f-]{36}__", "");
    if (!matches(definition.getFilenamePattern(), originalName)) errors.add("filename must match " + definition.getFilenamePattern());
    List<String> headers = readHeader(staged);
    List<String> missing = definition.getRequiredColumns().stream().filter(column -> !headers.contains(column.toLowerCase(Locale.ROOT))).toList();
    if (!missing.isEmpty()) errors.add("missing columns: " + String.join(", ", missing));
    String outcome = errors.isEmpty() ? "accepted" : "rejected";
    Path destination = directory(outcome, dataset).resolve(staged.getFileName());
    try {
      Files.move(staged, destination, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not route validated file", exception);
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("dataset", dataset);
    result.put("filename", originalName);
    result.put("status", outcome.toUpperCase(Locale.ROOT));
    result.put("errors", errors);
    result.put("storedAt", destination.toAbsolutePath().normalize().toString());
    result.put("processedAt", Instant.now().toString());
    if (errors.isEmpty()) {
      String acceptedAt = Instant.now().toString();
      String fingerprint = fingerprint(destination);
      String eventId = UUID.randomUUID().toString();
      boolean published = events.publish(new AcceptedFileEvent(eventId, "accepted-file-v1", dataset,
          originalName, destination.toAbsolutePath().normalize().toString(), fingerprint, acceptedAt));
      result.put("eventId", eventId);
      result.put("fingerprint", fingerprint);
      result.put("deliveryMode", published ? events.mode() : "FILE_WATCHER_FALLBACK");
      result.put("eventPublished", published);
    }
    return result;
  }

  private void importSample(String dataset, Path sample) {
    Path staged = directory("inbound", dataset).resolve(uniqueName(sample.getFileName().toString()));
    try {
      Files.copy(sample, staged, StandardCopyOption.REPLACE_EXISTING);
      validateAndRoute(dataset, staged);
    } catch (IOException exception) {
      throw new IllegalStateException("Could not import sample file " + sample.getFileName(), exception);
    }
  }

  private List<String> readHeader(Path file) {
    try (var reader = new BufferedReader(new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      if (line == null || line.isBlank()) return List.of();
      return parseCsvLine(line).stream().map(value -> value.trim().toLowerCase(Locale.ROOT)).toList();
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Could not read CSV header", exception);
    }
  }

  private List<String> parseCsvLine(String line) {
    var values = new ArrayList<String>();
    var current = new StringBuilder();
    boolean quoted = false;
    for (int index = 0; index < line.length(); index++) {
      char character = line.charAt(index);
      if (character == '"' && quoted && index + 1 < line.length() && line.charAt(index + 1) == '"') { current.append('"'); index++; }
      else if (character == '"') quoted = !quoted;
      else if (character == ',' && !quoted) { values.add(current.toString()); current.setLength(0); }
      else current.append(character);
    }
    values.add(current.toString());
    return values;
  }

  private FileInputProperties.Dataset requireDataset(String dataset) {
    var definition = properties.getDatasets().get(dataset);
    if (definition == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown dataset: " + dataset);
    return definition;
  }

  private Path directory(String zone, String dataset) { return properties.getRootDirectory().toAbsolutePath().normalize().resolve(zone).resolve(dataset); }
  private String uniqueName(String filename) { return UUID.randomUUID() + "__" + filename; }
  private String safeFilename(String filename) {
    if (filename == null || filename.isBlank()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Filename is required");
    String safe = Path.of(filename).getFileName().toString();
    if (!safe.equals(filename) || safe.contains("..")) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsafe filename");
    return safe;
  }
  private boolean matches(String pattern, String filename) { return FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Path.of(filename)); }
  private String fingerprint(Path file) {
    try (var input = Files.newInputStream(file)) {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] buffer = new byte[8192];
      int read;
      while ((read = input.read(buffer)) >= 0) if (read > 0) digest.update(buffer, 0, read);
      return HexFormat.of().formatHex(digest.digest());
    } catch (IOException | NoSuchAlgorithmException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not fingerprint accepted file", exception);
    }
  }
  private long countFiles(Path directory) {
    try (Stream<Path> files = Files.exists(directory) ? Files.list(directory) : Stream.empty()) { return files.filter(Files::isRegularFile).count(); }
    catch (IOException exception) { throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not inspect input directory", exception); }
  }
  private boolean containsOriginalFile(Path directory, String filename) {
    try (Stream<Path> files = Files.exists(directory) ? Files.list(directory) : Stream.empty()) {
      return files.filter(Files::isRegularFile).anyMatch(path -> path.getFileName().toString().equals(filename)
          || path.getFileName().toString().endsWith("__" + filename));
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not inspect input directory", exception);
    }
  }
}
