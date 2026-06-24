# Kafka Job Pipeline

A distributed, fault-tolerant job processing pipeline built with Spring Boot, Apache Kafka, and Redis. The system supports asynchronous job execution, status tracking, automatic retries with exponential backoff and jitter, dead-letter queuing (DLQ), and automated state cleanup.

## System Architecture

The pipeline consists of the following components:

- **job-submission-service**: Exposes HTTP endpoints to submit tasks and track status. It registers initial task state in Redis and publishes events to the `jobs` topic in Kafka.
- **worker-service**: Consumes tasks from Kafka, executes processing logic based on job type, updates status in Redis, and handles retries or DLQ routing on failures.
- **Kafka**: Serves as the message broker, orchestrating tasks across three topics:
  - `jobs`: Main task ingestion topic (3 partitions).
  - `jobs.retry`: Retry topic for failed attempts (3 partitions).
  - `jobs.dlq`: Dead-letter queue for tasks exceeding maximum retry attempts (1 partition).
- **Redis**: Serves as a fast, transient state store tracking job lifecycle states and outputs.

## Technology Stack

- Java 17
- Spring Boot 3.4.2
- Spring Kafka
- Spring Data Redis
- Apache Kafka (Confluent 7.5.0)
- Redis 7 (Alpine)
- Docker Compose

## Job Types

The pipeline processes three job types with simulated execution delays:
- `PDF_GENERATION`: 4 seconds
- `IMAGE_RESIZE`: 10 seconds
- `DATA_EXPORT`: 2 seconds

## API Specifications

### Job Submission Service (Port 8080)

#### Submit a Job
- **Endpoint**: `POST /api/jobs`
- **Request Body**:
  ```json
  {
    "jobType": "PDF_GENERATION",
    "payload": {
      "key": "value"
    }
  }
  ```
- **Response**: `201 Created`
  ```json
  {
    "jobId": "f47ac10b-58cc-4372-a567-0e02b2c3d479"
  }
  ```

#### Fetch Job Status
- **Endpoint**: `GET /api/jobs/{jobId}/status`
- **Response**: `200 OK`
  ```json
  {
    "submittedAt": "1782352800000",
    "jobType": "PDF_GENERATION",
    "status": "DONE",
    "result": "https://storage/PDF_GENERATION/f47ac10b-58cc-4372-a567-0e02b2c3d479.output"
  }
  ```

### Worker Service (Port 8081)

#### Preview Eligible Cleanup Keys
- **Endpoint**: `GET /admin/cleanup/preview`
- **Response**: `200 OK`
  ```json
  {
    "status": "SUCCESS",
    "dryRun": true,
    "eligibleKeyCount": 0,
    "eligibleKeys": []
  }
  ```

## Fault Tolerance & Retry Logic

1. **First Attempt**: Tasks are consumed from `jobs`. If processing fails, the attempt count is checked.
2. **Retry Route**: If `currentAttempts < maxAttempts` (default max is 3), the task is published to `jobs.retry` with an updated attempt count and the last error message.
3. **Exponential Backoff**: The `RetryConsumer` waits before reprocessing using the formula:
   `waitMs = 2^(attempt - 1) * 1000 + randomJitter` (where `randomJitter` is 0 to 500ms).
4. **Dead-Letter Queue (DLQ)**: If a job fails all 3 attempts, it is routed to `jobs.dlq`, and its state in Redis is set to `FAILED` with the corresponding error log.

### Simulating Failures
To test the retry mechanism and DLQ routing, submit a job that generates a UUID ending with the character `0`. The processing service will automatically trigger a simulated exception.

## Database Cleanup

To prevent Redis from bloating, a scheduled cleanup job (`CleanScheduler`) runs every 60 seconds. It scans keys matching `job:*` using a sticky Redis connection (`SCAN` command) and deletes keys that:
- Have a status of `DONE` or `FAILED`.
- Were submitted more than 24 hours ago.

## Getting Started

### Prerequisites
- Docker and Docker Compose
- JDK 17
- Maven 3.x

### Run Infrastructure
Start Kafka, Zookeeper, and Redis using Docker Compose:
```bash
docker-compose up -d
```
This automatically initializes the required Kafka topics (`jobs`, `jobs.retry`, `jobs.dlq`).

### Run Services Locally
Run each Spring Boot application using the Maven wrapper:

```bash
# In job-submission-service directory
./mvnw spring-boot:run

# In worker-service directory
./mvnw spring-boot:run
```