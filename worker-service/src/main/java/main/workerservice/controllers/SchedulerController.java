package main.workerservice.controllers;

import lombok.RequiredArgsConstructor;
import main.workerservice.schedulers.CleanScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@RestController
public class SchedulerController {

    private final CleanScheduler cleanScheduler;

    @GetMapping("/admin/cleanup/preview")
    public ResponseEntity<Map<String,Object>> cleanupPreview(){
        List<String> expiredKeys = cleanScheduler.getExpiredKeys();

        Map<String,Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("dryRun", true);
        response.put("eligibleKeyCount", expiredKeys.size());
        response.put("eligibleKeys", expiredKeys);

        return ResponseEntity.ok(response);
    }
}
