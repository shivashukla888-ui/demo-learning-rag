package com.aegis.surveillance.config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "aegis.file-input")
public class FileInputProperties {
  private Path rootDirectory = Path.of("./data/input");
  private Path sampleDirectory = Path.of("./sample-data");
  private long maxFileSizeBytes = 26_214_400;
  private boolean createDirectories = true;
  private boolean bootstrapSamples = true;
  private Map<String, Dataset> datasets = new LinkedHashMap<>();

  public Path getRootDirectory() { return rootDirectory; }
  public void setRootDirectory(Path rootDirectory) { this.rootDirectory = rootDirectory; }
  public Path getSampleDirectory() { return sampleDirectory; }
  public void setSampleDirectory(Path sampleDirectory) { this.sampleDirectory = sampleDirectory; }
  public long getMaxFileSizeBytes() { return maxFileSizeBytes; }
  public void setMaxFileSizeBytes(long maxFileSizeBytes) { this.maxFileSizeBytes = maxFileSizeBytes; }
  public boolean isCreateDirectories() { return createDirectories; }
  public void setCreateDirectories(boolean createDirectories) { this.createDirectories = createDirectories; }
  public boolean isBootstrapSamples() { return bootstrapSamples; }
  public void setBootstrapSamples(boolean bootstrapSamples) { this.bootstrapSamples = bootstrapSamples; }
  public Map<String, Dataset> getDatasets() { return datasets; }
  public void setDatasets(Map<String, Dataset> datasets) { this.datasets = datasets; }

  public static class Dataset {
    private boolean required;
    private String filenamePattern = "*.csv";
    private String bootstrapFilename = "";
    private List<String> requiredColumns = List.of();

    public boolean isRequired() { return required; }
    public void setRequired(boolean required) { this.required = required; }
    public String getFilenamePattern() { return filenamePattern; }
    public void setFilenamePattern(String filenamePattern) { this.filenamePattern = filenamePattern; }
    public String getBootstrapFilename() { return bootstrapFilename; }
    public void setBootstrapFilename(String bootstrapFilename) { this.bootstrapFilename = bootstrapFilename; }
    public List<String> getRequiredColumns() { return requiredColumns; }
    public void setRequiredColumns(List<String> requiredColumns) { this.requiredColumns = requiredColumns; }
  }
}
