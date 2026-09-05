package com.aegis.surveillance.controller;

import com.aegis.surveillance.service.FileInputService;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/v1/ingestion/files")
public class FileInputController {
  private final FileInputService files;

  public FileInputController(FileInputService files) { this.files = files; }

  @GetMapping("/configuration")
  public Map<String, Object> configuration() { return files.configuration(); }

  @GetMapping("/readiness")
  public Map<String, Object> readiness() { return files.readiness(); }

  @PostMapping("/{dataset}")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Map<String, Object> upload(@PathVariable String dataset, @RequestPart("file") MultipartFile file) {
    return files.upload(dataset, file);
  }

  @PostMapping("/scan")
  public List<Map<String, Object>> scan() { return files.scanPending(); }
}
