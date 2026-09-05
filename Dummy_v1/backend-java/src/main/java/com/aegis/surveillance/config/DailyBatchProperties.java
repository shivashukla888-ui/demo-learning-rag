package com.aegis.surveillance.config;

import java.nio.file.Path;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "surveillance.daily-batch")
public class DailyBatchProperties {
  private Path rootDirectory = Path.of("./data/daily-input");
  private List<String> allowedRegions = List.of("AMER", "EMEA", "APAC", "GLOBAL");
  private long maxAlertBytes = 536_870_912L;
  private long maxParquetBytes = 21_474_836_480L;

  public Path getRootDirectory() { return rootDirectory; }
  public void setRootDirectory(Path rootDirectory) { this.rootDirectory = rootDirectory; }
  public List<String> getAllowedRegions() { return allowedRegions; }
  public void setAllowedRegions(List<String> allowedRegions) { this.allowedRegions = allowedRegions; }
  public long getMaxAlertBytes() { return maxAlertBytes; }
  public void setMaxAlertBytes(long maxAlertBytes) { this.maxAlertBytes = maxAlertBytes; }
  public long getMaxParquetBytes() { return maxParquetBytes; }
  public void setMaxParquetBytes(long maxParquetBytes) { this.maxParquetBytes = maxParquetBytes; }
}
