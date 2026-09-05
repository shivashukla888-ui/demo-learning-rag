package com.aegis.surveillance.service;

import com.aegis.surveillance.config.DailyBatchProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@Service
public class DailyBatchService {
  private static final Set<String> DATASETS = Set.of("alerts", "trades");
  private static final Set<String> ASSET_CLASSES = Set.of(
      "FIXED_INCOME", "FOREIGN_EXCHANGE", "INTEREST_RATE_DERIVATIVES", "CREDIT_DERIVATIVES");
  private static final List<String> REQUIRED_ALERT_FIELDS = List.of(
      "alertId", "ruleId", "ruleVersion", "typology", "assetClass", "businessDate", "region", "triggeringTradeIds");
  private final DailyBatchProperties properties;
  private final ObjectMapper json;

  public DailyBatchService(DailyBatchProperties properties, ObjectMapper json) {
    this.properties = properties;
    this.json = json;
  }

  @PostConstruct
  public void initialise() {
    try { Files.createDirectories(properties.getRootDirectory()); }
    catch (IOException exception) { throw new IllegalStateException("Could not initialise daily batch input", exception); }
  }

  public Map<String, Object> configuration() {
    return Map.of(
        "mode", "DAILY_PAIRED_BATCH",
        "rootDirectory", properties.getRootDirectory().toAbsolutePath().normalize().toString(),
        "allowedRegions", properties.getAllowedRegions(),
        "requiredFiles", List.of("one alerts.json or alerts.jsonl", "one or more .parquet files"),
        "maxAlertBytes", properties.getMaxAlertBytes(),
        "maxParquetBytes", properties.getMaxParquetBytes(),
        "originalJavaAlertsPreserved", true,
        "automaticClosureAllowed", false);
  }

  public Map<String, Object> upload(String regionValue, LocalDate businessDate, String batchIdValue,
                                    String datasetValue, MultipartFile file) {
    return uploadOne(regionValue, businessDate, batchIdValue, datasetValue, file, true);
  }

  public Map<String, Object> uploadParquetFiles(String regionValue, LocalDate businessDate,
                                                 String batchIdValue, List<MultipartFile> files) {
    if (files == null || files.isEmpty())
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "At least one Parquet file is required");
    String region = requireRegion(regionValue);
    String batchId = requireBatchId(batchIdValue);
    Path batch = batchDirectory(region, businessDate, batchId);
    ensureBatchOpen(batch);
    var accepted = new ArrayList<Map<String, Object>>();
    deleteStagedParquet(batch);
    try {
      var fingerprints = new HashSet<String>();
      for (MultipartFile file : files) {
        Map<String, Object> item = uploadOne(region, businessDate, batchId, "trades", file, false);
        if (!fingerprints.add(item.get("fingerprint").toString()))
          throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
              "The Parquet list contains duplicate file content");
        accepted.add(item);
      }
    } catch (RuntimeException exception) {
      deleteStagedParquet(batch);
      throw exception;
    }
    try {
      Map<String, Object> readiness = writeReadyManifestIfComplete(region, businessDate, batchId, batch);
      var result = new LinkedHashMap<String, Object>();
      result.put("batchId", batchId); result.put("businessDate", businessDate.toString()); result.put("region", region);
      result.put("dataset", "trades"); result.put("status", "ACCEPTED"); result.put("fileCount", accepted.size());
      result.put("files", accepted); result.put("readiness", readiness);
      return result;
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
          "Could not finalise the daily Parquet batch: " + exception.getMessage(), exception);
    }
  }

  private Map<String, Object> uploadOne(String regionValue, LocalDate businessDate, String batchIdValue,
                                        String datasetValue, MultipartFile file, boolean finalise) {
    String region = requireRegion(regionValue);
    String batchId = requireBatchId(batchIdValue);
    String dataset = datasetValue.toLowerCase(Locale.ROOT);
    if (!DATASETS.contains(dataset)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Dataset must be alerts or trades");
    if (file == null || file.isEmpty()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File is required");
    long limit = dataset.equals("alerts") ? properties.getMaxAlertBytes() : properties.getMaxParquetBytes();
    if (file.getSize() > limit) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "File exceeds configured size limit");
    String original = safeFilename(file.getOriginalFilename());
    validateExtension(dataset, original);
    Path batch = batchDirectory(region, businessDate, batchId);
    try {
      Files.createDirectories(batch);
      ensureBatchOpen(batch);
      String extension = original.substring(original.lastIndexOf('.')).toLowerCase(Locale.ROOT);
      Path temporary = batch.resolve("." + dataset + "-" + UUID.randomUUID() + ".upload");
      Files.copy(file.getInputStream(), temporary, StandardCopyOption.REPLACE_EXISTING);
      Path destination;
      try {
        if (dataset.equals("alerts")) {
          validateAlerts(temporary, region, businessDate, batchId);
          destination = batch.resolve("alerts" + extension);
          Files.deleteIfExists(batch.resolve(extension.equals(".json") ? "alerts.jsonl" : "alerts.json"));
        } else {
          validateParquetEnvelope(temporary);
          destination = batch.resolve("trades-" + fingerprint(temporary).substring(0, 16) + ".parquet");
        }
        Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (Exception exception) {
        Files.deleteIfExists(temporary);
        throw exception;
      }
      Map<String, Object> readiness = finalise
          ? writeReadyManifestIfComplete(region, businessDate, batchId, batch)
          : readinessFor(region, businessDate, batchId, batch);
      var result = new LinkedHashMap<String, Object>();
      result.put("batchId", batchId); result.put("businessDate", businessDate.toString()); result.put("region", region);
      result.put("dataset", dataset); result.put("filename", original); result.put("status", "ACCEPTED");
      result.put("fingerprint", fingerprint(destination)); result.put("readiness", readiness);
      return result;
    } catch (ResponseStatusException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
          "Daily " + dataset + " file failed validation: " + exception.getMessage(), exception);
    }
  }

  public Map<String, Object> readiness(String regionValue, LocalDate businessDate, String batchIdValue) {
    String region = requireRegion(regionValue);
    String batchId = requireBatchId(batchIdValue);
    return readinessFor(region, businessDate, batchId, batchDirectory(region, businessDate, batchId));
  }

  public List<Map<String, Object>> batches() {
    var batches = new ArrayList<Map<String, Object>>();
    Path root = properties.getRootDirectory();
    if (!Files.isDirectory(root)) return batches;
    try (Stream<Path> regions = Files.list(root)) {
      regions.filter(Files::isDirectory).sorted().forEach(regionPath -> {
        try (Stream<Path> dates = Files.list(regionPath)) {
          dates.filter(Files::isDirectory).sorted().forEach(datePath -> {
            try (Stream<Path> ids = Files.list(datePath)) {
              ids.filter(Files::isDirectory).sorted().forEach(batchPath -> {
                String region = regionPath.getFileName().toString().replaceFirst("^region=", "");
                String date = datePath.getFileName().toString().replaceFirst("^business_date=", "");
                String batch = batchPath.getFileName().toString().replaceFirst("^batch_id=", "");
                try { batches.add(readinessFor(region, LocalDate.parse(date), batch, batchPath)); }
                catch (RuntimeException ignored) { /* Invalid operator-created paths are not advertised. */ }
              });
            } catch (IOException ignored) { }
          });
        } catch (IOException ignored) { }
      });
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not inspect daily batches", exception);
    }
    return batches;
  }

  private void validateAlerts(Path path, String expectedRegion, LocalDate expectedDate, String expectedBatchId) throws IOException {
    int records = 0;
    try (JsonParser parser = json.getFactory().createParser(path.toFile())) {
      JsonToken token = parser.nextToken();
      boolean array = token == JsonToken.START_ARRAY;
      if (array) token = parser.nextToken();
      while (token != null && token != JsonToken.END_ARRAY) {
        JsonNode alert = json.readTree(parser);
        validateAlert(alert, expectedRegion, expectedDate, expectedBatchId, records + 1);
        records++;
        token = parser.nextToken();
      }
    }
    if (records == 0) throw new IllegalArgumentException("Alert file contains no records");
  }

  private void validateAlert(JsonNode alert, String expectedRegion, LocalDate expectedDate, String expectedBatchId, int record) {
    if (alert == null || !alert.isObject()) throw new IllegalArgumentException("Alert record " + record + " is not an object");
    List<String> missing = REQUIRED_ALERT_FIELDS.stream()
        .filter(field -> !alert.hasNonNull(field) || (alert.get(field).isTextual() && alert.get(field).asText().isBlank())).toList();
    if (!missing.isEmpty()) throw new IllegalArgumentException("Alert record " + record + " missing fields: " + String.join(", ", missing));
    if (!expectedRegion.equalsIgnoreCase(alert.get("region").asText()))
      throw new IllegalArgumentException("Alert record " + record + " region does not match upload region");
    if (!expectedDate.toString().equals(alert.get("businessDate").asText()))
      throw new IllegalArgumentException("Alert record " + record + " businessDate does not match upload date");
    if (alert.hasNonNull("batchId") && !expectedBatchId.equals(alert.get("batchId").asText()))
      throw new IllegalArgumentException("Alert record " + record + " batchId does not match upload batch");
    if (!ASSET_CLASSES.contains(alert.get("assetClass").asText().toUpperCase(Locale.ROOT)))
      throw new IllegalArgumentException("Alert record " + record + " uses an unsupported assetClass");
    if (!alert.get("triggeringTradeIds").isArray() || alert.get("triggeringTradeIds").isEmpty())
      throw new IllegalArgumentException("Alert record " + record + " requires triggeringTradeIds");
  }

  private void validateParquetEnvelope(Path path) throws IOException {
    if (Files.size(path) < 8) throw new IllegalArgumentException("Parquet file is too small");
    byte[] prefix = new byte[4]; byte[] suffix = new byte[4];
    try (InputStream input = Files.newInputStream(path)) {
      if (input.read(prefix) != 4) throw new IllegalArgumentException("Could not read Parquet header");
    }
    try (var channel = Files.newByteChannel(path)) {
      channel.position(Files.size(path) - 4); channel.read(java.nio.ByteBuffer.wrap(suffix));
    }
    if (!"PAR1".equals(new String(prefix, java.nio.charset.StandardCharsets.US_ASCII))
        || !"PAR1".equals(new String(suffix, java.nio.charset.StandardCharsets.US_ASCII)))
      throw new IllegalArgumentException("File does not contain a valid Parquet envelope");
  }

  private Map<String, Object> writeReadyManifestIfComplete(String region, LocalDate date, String batchId, Path batch) throws IOException {
    Path alerts = findAlerts(batch);
    List<Path> trades = findParquetFiles(batch);
    if (alerts == null || trades.isEmpty()) return readinessFor(region, date, batchId, batch);
    Map<String, Object> manifest = Map.of(
        "schemaVersion", "daily-paired-batch-v1", "batchId", batchId, "businessDate", date.toString(), "region", region,
        "status", "READY", "createdAt", Instant.now().toString(),
        "alerts", fileDescriptor(alerts), "trades", fileDescriptors(trades),
        "privacy", Map.of("identifiers", "TOKENISED", "llmRawDataAllowed", false));
    Path temporary = batch.resolve(".manifest.ready.json.tmp");
    json.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), manifest);
    Files.move(temporary, batch.resolve("manifest.ready.json"), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    return readinessFor(region, date, batchId, batch);
  }

  private Map<String, Object> readinessFor(String region, LocalDate date, String batchId, Path batch) {
    boolean alerts = findAlerts(batch) != null;
    int parquetFiles = findParquetFiles(batch).size();
    boolean parquet = parquetFiles > 0;
    boolean ready = alerts && parquet && Files.isRegularFile(batch.resolve("manifest.ready.json"));
    return Map.of("batchId", batchId, "businessDate", date.toString(), "region", region,
        "alertsAvailable", alerts, "parquetAvailable", parquet, "parquetFiles", parquetFiles, "ready", ready,
        "status", ready ? "READY" : alerts && parquet ? "STAGED_NOT_FINALISED" : "WAITING_FOR_PAIR",
        "checkedAt", Instant.now().toString());
  }

  private Path findAlerts(Path batch) {
    Path jsonl = batch.resolve("alerts.jsonl");
    if (Files.isRegularFile(jsonl)) return jsonl;
    Path jsonFile = batch.resolve("alerts.json");
    return Files.isRegularFile(jsonFile) ? jsonFile : null;
  }

  private List<Path> findParquetFiles(Path batch) {
    if (!Files.isDirectory(batch)) return List.of();
    try (Stream<Path> files = Files.list(batch)) {
      return files.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().matches("trades-[0-9a-f]{16}\\.parquet"))
          .sorted().toList();
    } catch (IOException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not inspect Parquet files", exception);
    }
  }

  private void deleteStagedParquet(Path batch) {
    for (Path path : findParquetFiles(batch)) {
      try { Files.deleteIfExists(path); }
      catch (IOException exception) {
        throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not replace staged Parquet files", exception);
      }
    }
  }

  private List<Map<String, Object>> fileDescriptors(List<Path> files) throws IOException {
    var descriptors = new ArrayList<Map<String, Object>>();
    for (Path file : files) descriptors.add(fileDescriptor(file));
    return descriptors;
  }

  private void ensureBatchOpen(Path batch) {
    if (Files.isRegularFile(batch.resolve("manifest.ready.json")))
      throw new ResponseStatusException(HttpStatus.CONFLICT,
          "Batch is already READY and immutable; use a new batchId for replacement data");
  }

  private Map<String, Object> fileDescriptor(Path file) throws IOException {
    return Map.of("filename", file.getFileName().toString(), "bytes", Files.size(file), "sha256", fingerprint(file));
  }

  private Path batchDirectory(String region, LocalDate date, String batchId) {
    return properties.getRootDirectory().toAbsolutePath().normalize()
        .resolve("region=" + region).resolve("business_date=" + date).resolve("batch_id=" + batchId);
  }
  private String requireRegion(String value) {
    String region = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    boolean allowed = properties.getAllowedRegions().stream().anyMatch(item -> item.equalsIgnoreCase(region));
    if (!allowed) throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
        "Region must be one of " + String.join(", ", properties.getAllowedRegions()));
    return region;
  }
  private String requireBatchId(String value) {
    if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9._-]{2,79}"))
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid batchId");
    return value;
  }
  private void validateExtension(String dataset, String name) {
    String lower = name.toLowerCase(Locale.ROOT);
    boolean valid = dataset.equals("alerts") ? lower.endsWith(".json") || lower.endsWith(".jsonl") : lower.endsWith(".parquet");
    if (!valid) throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
        dataset.equals("alerts") ? "Alert file must use .json or .jsonl" : "Trade input must use .parquet");
  }
  private String safeFilename(String filename) {
    if (filename == null || filename.isBlank()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Filename is required");
    String safe = Path.of(filename).getFileName().toString();
    if (!safe.equals(filename) || safe.contains("..")) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsafe filename");
    return safe;
  }
  private String fingerprint(Path file) {
    try (InputStream input = Files.newInputStream(file)) {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] buffer = new byte[1 << 20]; int read;
      while ((read = input.read(buffer)) >= 0) if (read > 0) digest.update(buffer, 0, read);
      return HexFormat.of().formatHex(digest.digest());
    } catch (IOException | NoSuchAlgorithmException exception) {
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not fingerprint file", exception);
    }
  }
}
