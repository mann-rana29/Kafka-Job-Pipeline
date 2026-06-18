package main.workerservice.schedulers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class CleanScheduler {

    private final RedisTemplate<String,Object> redisTemplate;

    @Scheduled(fixedRate = 60000)
    public void performCleaning(){
      log.info("Background cleanup job started!");

      List<String> keysToDelete = getExpiredKeys();

      if(!keysToDelete.isEmpty()){
          redisTemplate.delete(keysToDelete);
          log.info("Cleanup job finished. Found {} expired jobs.. Deleted {} job records.", keysToDelete.size(),keysToDelete.size());
      }else{
          log.info("Cleanup job finished. No expired records found.");
      }
    }

    public List<String> getExpiredKeys(){
        long cutOffTime = System.currentTimeMillis() - (24*60*60*1000);
        List<String> keysToDelete = new ArrayList<>();

        redisTemplate.execute((RedisCallback<Void>) connection -> {
            ScanOptions options = ScanOptions.scanOptions().match("job:*").count(100).build();
            Cursor<byte[]> cursor = redisTemplate.executeWithStickyConnection(redis -> redis.scan(options));

            while(cursor.hasNext()){
                String key = new String(cursor.next());

                String status = (String) redisTemplate.opsForHash().get(key,"status");
                String submittedAtStr = (String) redisTemplate.opsForHash().get(key,"submittedAt");

                if(submittedAtStr != null && status != null){
                    long submittedAt = Long.parseLong(submittedAtStr);

                    if(submittedAt < cutOffTime && ("DONE".equals(status) || "FAILED".equals(status))){
                        keysToDelete.add(key);
                    }
                }
            }

            return null;
        });

        return keysToDelete;
    }
}
