# Merge Worker Architecture Documentation

## Overview

This commit (`de01a772891a200c64b2e5ce5125fd6be1f4c357`) introduces an **offline audio merge worker system** for LiveKit Egress. The system enables recording individual participant audio tracks during a LiveKit session, then merging them into a single mixed audio file after the session ends.

## Key Components

### 1. Audio Recording Pipeline
- Records individual participant audio tracks in real-time
- Captures clock synchronization information for offline alignment
- Supports encryption (AES-256-GCM envelope encryption)
- Generates manifest files with metadata

### 2. Merge Worker
- Processes merge jobs from a Redis queue
- Downloads participant audio files
- Aligns tracks using clock sync information
- Merges tracks using GStreamer
- Uploads merged files and updates manifest

### 3. Encryption System
- Envelope encryption (AES-256-GCM)
- Per-file Data Encryption Keys (DEK)
- Master Key Encryption (KEK) for DEK protection
- Streaming encryption support for large files

### 4. Queue System
- Redis-based job queue
- Job status tracking
- Automatic retry mechanism (max 3 retries)
- Worker claim/visibility timeout

## System Architecture

```mermaid
graph TB
    subgraph "LiveKit Session"
        Room[LiveKit Room]
        P1[Participant 1]
        P2[Participant 2]
        P3[Participant 3]
    end

    subgraph "Egress Handler Process"
        SDK[SDK Source]
        AppWriter[RecordingAppWriter]
        Pipeline[GStreamer Pipeline]
        Sink[AudioRecordingSink]
    end

    subgraph "Object Storage"
        Storage[(Object Storage<br/>S3/GCS/Azure)]
        Manifest[manifest.json]
        P1File[Participant 1 Audio]
        P2File[Participant 2 Audio]
        P3File[Participant 3 Audio]
        MergedFile[Room Mix Audio]
        UpdatedManifest[Updated manifest.json]
    end

    subgraph "Merge Worker System"
        Queue[(Redis Queue)]
        Worker1[Merge Worker 1]
        Worker2[Merge Worker 2]
        Worker3[Merge Worker 3]
    end

    Room --> P1
    Room --> P2
    Room --> P3

    P1 --> SDK
    P2 --> SDK
    P3 --> SDK

    SDK --> AppWriter
    AppWriter --> Pipeline
    Pipeline --> Sink

    Sink -->|Upload| Storage
    Storage -.-> Manifest
    Storage -.-> P1File
    Storage -.-> P2File
    Storage -.-> P3File

    Sink -.->|Enqueue Job| Queue

    Queue -->|Dequeue| Worker1
    Queue -->|Dequeue| Worker2
    Queue -->|Dequeue| Worker3

    Worker1 -->|Download| Storage
    Worker2 -->|Download| Storage
    Worker3 -->|Download| Storage

    Worker1 -->|Upload| MergedFile
    Worker2 -->|Upload| MergedFile
    Worker3 -->|Upload| MergedFile

    Worker1 -->|Upload| UpdatedManifest
    Worker2 -->|Upload| UpdatedManifest
    Worker3 -->|Upload| UpdatedManifest
```

## Recording Flow

```mermaid
sequenceDiagram
    participant Room as LiveKit Room
    participant Handler as Egress Handler
    participant AppWriter as RecordingAppWriter
    participant Pipeline as GStreamer Pipeline
    participant Sink as AudioRecordingSink
    participant Storage as Object Storage
    participant Queue as Redis Queue

    Room->>Handler: Participant joins
    Handler->>AppWriter: Create RecordingAppWriter
    AppWriter->>AppWriter: Initialize ring buffer (20ms)
    AppWriter->>AppWriter: Setup clock sync capture

    loop For each RTP packet
        Room->>AppWriter: RTP packet
        AppWriter->>AppWriter: Capture clock sync info<br/>(first packet only)
        AppWriter->>AppWriter: Buffer packet (ring buffer)
        AppWriter->>Pipeline: Push to GStreamer
        Pipeline->>Sink: Encoded audio data
    end

    Sink->>Sink: Write to encrypted temp file
    Sink->>Sink: Update manifest with participant info

    Room->>Handler: Participant leaves
    Handler->>Sink: RemoveParticipant()
    Sink->>Sink: Finalize participant file
    Sink->>Sink: Calculate SHA256 checksum
    Sink->>Storage: Upload participant file
    Sink->>Sink: Add artifact to manifest

    Room->>Handler: Session ends
    Handler->>Sink: Close()
    Sink->>Storage: Upload manifest.json
    Sink->>Queue: Enqueue merge job (if FinalRoomMix enabled)
```

## Merge Worker Flow

```mermaid
sequenceDiagram
    participant Queue as Redis Queue
    participant Worker as Merge Worker
    participant Storage as Object Storage
    participant LocalFS as Local Temp FS
    participant GStreamer as GStreamer
    participant Align as Alignment Engine

    Worker->>Queue: Poll for jobs (every 5s)
    Queue->>Worker: Dequeue job (LPOP)
    Worker->>Queue: Claim job (status: running)

    Worker->>Storage: Download manifest.json
    Storage->>Worker: Manifest data
    Worker->>LocalFS: Save manifest

    Worker->>Align: ComputeAlignment(manifest)
    Align->>Align: Find earliest server timestamp
    Align->>Align: Calculate offsets per participant
    Align->>Align: Estimate clock drift
    Align->>Worker: AlignmentResult

    loop For each participant
        Worker->>Storage: Download participant audio file
        Storage->>Worker: Encrypted audio file
        Worker->>LocalFS: Save to temp dir
    end

    Worker->>GStreamer: Build merge pipeline
    Note over GStreamer: For each participant:<br/>filesrc → decodebin →<br/>audioconvert → audioresample →<br/>identity(ts-offset) → audiomixer
    
    Note over GStreamer: audiomixer → audioconvert →<br/>audioresample → encoder → filesink

    GStreamer->>LocalFS: Write merged file(s)

    loop For each format (OGG, WAV)
        Worker->>LocalFS: Read merged file
        Worker->>Worker: Calculate SHA256 checksum
        Worker->>Storage: Upload merged file
        Worker->>Worker: Update manifest with artifact
    end

    Worker->>Storage: Upload updated manifest.json
    Worker->>Queue: Complete job (status: completed)
```

## Alignment Algorithm

The alignment algorithm ensures that participant audio tracks are properly synchronized when merged, accounting for:
- Different join times
- Clock drift between participants
- RTP timestamp differences

```mermaid
graph TB
    Start[Start: AudioRecordingManifest] --> Extract[Extract All Participants]
    
    Extract --> P1[Participant 1<br/>ServerTimestamp: T1<br/>RTPClock: C1]
    Extract --> P2[Participant 2<br/>ServerTimestamp: T2<br/>RTPClock: C2]
    Extract --> P3[Participant 3<br/>ServerTimestamp: T3<br/>RTPClock: C3]
    
    P1 --> FindRef[Find Reference Start<br/>min ServerTimestamp<br/>or JoinedAt]
    P2 --> FindRef
    P3 --> FindRef
    
    FindRef --> Ref[ReferenceStart = earliest time]
    
    Ref --> Process1[Process Participant 1<br/>Offset = T1 - Ref<br/>Duration from artifacts]
    Ref --> Process2[Process Participant 2<br/>Offset = T2 - Ref<br/>Duration from artifacts]
    Ref --> Process3[Process Participant 3<br/>Offset = T3 - Ref<br/>Duration from artifacts]
    
    Process1 --> Drift1[Estimate Clock Drift 1<br/>drift = duration_hours × 10ms]
    Process2 --> Drift2[Estimate Clock Drift 2<br/>drift = duration_hours × 10ms]
    Process3 --> Drift3[Estimate Clock Drift 3<br/>drift = duration_hours × 10ms]
    
    Drift1 --> Collect[Collect All Alignments]
    Drift2 --> Collect
    Drift3 --> Collect
    
    Collect --> Sort[Sort Alignments by Offset]
    Sort --> CalcDuration[Calculate Total Duration<br/>max end time - ReferenceStart]
    CalcDuration --> Result[AlignmentResult<br/>ReferenceStart<br/>Alignments Array<br/>TotalDuration]
```

### Alignment Details

1. **Reference Point Selection**: Uses the earliest server timestamp as the reference point (time 0)
2. **Offset Calculation**: For each participant, calculates offset = `participant_start_time - reference_start_time`
3. **Clock Drift Estimation**: Estimates potential clock drift using formula: `drift = (duration_hours × 10ms)`
   - Target: <20ms drift over 60 minutes
   - Conservative estimate: 10ms per hour
4. **GStreamer Integration**: Offsets are applied using `identity` element's `ts-offset` property

## Encryption System

The system supports envelope encryption for local storage:

```mermaid
graph TB
    subgraph "Encryption Flow"
        MK[Master Key<br/>KEK - 32 bytes]
        DEK[Data Encryption Key<br/>DEK - 32 bytes<br/>Random per file]
        Data[Audio Data]
    end

    subgraph "Encryption Process"
        GenDEK[Generate Random DEK]
        EncData[Encrypt Data with DEK<br/>AES-256-GCM]
        EncDEK[Encrypt DEK with Master Key<br/>AES-256-GCM]
        Header[Build Header<br/>Magic + Version +<br/>Encrypted DEK + Nonces]
    end

    subgraph "Output File"
        File[Encrypted File<br/>Header + Encrypted Data]
    end

    MK --> EncDEK
    GenDEK --> DEK
    DEK --> EncData
    Data --> EncData
    EncDEK --> Header
    EncData --> File
    Header --> File
```

### Encryption File Format

```
[Header Section]
├── Magic: "LKAE" (4 bytes)
├── Version: 1 (1 byte)
├── Encrypted DEK Length: uint16 (2 bytes)
├── Encrypted DEK: variable length
├── DEK Nonce: 12 bytes
└── Base Data Nonce: 12 bytes

[Data Section - Chunked]
├── Chunk 1
│   ├── Chunk Nonce: 12 bytes (base nonce XOR counter)
│   ├── Encrypted Length: uint32 (4 bytes)
│   └── Encrypted Data: variable length
├── Chunk 2
│   └── ...
└── ...
```

### Decryption Flow

```mermaid
sequenceDiagram
    participant Reader as EncryptedTempFileReader
    participant File as Encrypted File
    participant Decryptor as EnvelopeEncryptor

    Reader->>File: Read header
    File->>Reader: Magic + Version + DEK length
    Reader->>File: Read encrypted DEK + nonces
    File->>Reader: Encrypted DEK + DEK nonce

    Reader->>Decryptor: Decrypt DEK (using Master Key)
    Decryptor->>Reader: Plaintext DEK

    loop For each chunk
        Reader->>File: Read chunk header
        File->>Reader: Chunk nonce + encrypted length
        Reader->>File: Read encrypted chunk data
        File->>Reader: Encrypted data
        
        Reader->>Decryptor: Decrypt chunk (using DEK)
        Decryptor->>Reader: Plaintext audio data
    end
```

## Queue System

The merge job queue is implemented using Redis:

```mermaid
stateDiagram-v2
    [*] --> Pending: Enqueue job
    Pending --> Running: Worker claims (LPOP)
    Running --> Completed: Merge succeeds
    Running --> Pending: Merge fails (retry < 3)
    Running --> Failed: Merge fails (retry >= 3)
    Completed --> [*]
    Failed --> [*]

    note right of Pending
        Redis List: livekit:egress:merge:queue
        FIFO order (RPUSH/LPOP)
    end note

    note right of Running
        Job claimed by worker
        Status stored in Redis
        Visibility timeout: 5 minutes
    end note

    note right of Failed
        Max retries: 3
        Error message stored
        Job marked as permanently failed
    end note
```

### Queue Operations

1. **Enqueue**: `RPUSH livekit:egress:merge:queue <job_json>`
2. **Dequeue**: `LPOP livekit:egress:merge:queue` (FIFO)
3. **Status Tracking**: `SET livekit:egress:merge:status:<job_id> <job_json> EX 86400`
4. **Requeue**: `LPUSH livekit:egress:merge:queue <job_json>` (for retries)

## GStreamer Merge Pipeline

The merge pipeline uses GStreamer's `audiomixer` element:

```mermaid
graph LR
    subgraph "Participant 1"
        F1[filesrc location=file1.ogg]
        D1[decodebin]
        AC1[audioconvert]
        AR1[audioresample]
        I1[identity ts-offset=offset1]
        M1[audiomixer.sink_1]
    end

    subgraph "Participant 2"
        F2[filesrc location=file2.ogg]
        D2[decodebin]
        AC2[audioconvert]
        AR2[audioresample]
        I2[identity ts-offset=offset2]
        M2[audiomixer.sink_2]
    end

    subgraph "Participant 3"
        F3[filesrc location=file3.ogg]
        D3[decodebin]
        AC3[audioconvert]
        AR3[audioresample]
        I3[identity ts-offset=offset3]
        M3[audiomixer.sink_3]
    end

    subgraph "Mixer & Output"
        MIX[audiomixer]
        AC_OUT[audioconvert]
        AR_OUT[audioresample]
        ENC[opusenc / wavenc]
        MUX[oggmux]
        SINK[filesink location=output]
    end

    F1 --> D1 --> AC1 --> AR1 --> I1 --> M1
    F2 --> D2 --> AC2 --> AR2 --> I2 --> M2
    F3 --> D3 --> AC3 --> AR3 --> I3 --> M3

    M1 --> MIX
    M2 --> MIX
    M3 --> MIX

    MIX --> AC_OUT --> AR_OUT --> ENC --> MUX --> SINK
```

### Pipeline Command Example

```bash
gst-launch-1.0 -e \
  filesrc location=participant1.ogg ! decodebin ! audioconvert ! audioresample ! \
  audio/x-raw,rate=48000,channels=2 ! identity ts-offset=0 ! audiomixer.sink_1 \
  filesrc location=participant2.ogg ! decodebin ! audioconvert ! audioresample ! \
  audio/x-raw,rate=48000,channels=2 ! identity ts-offset=500000000 ! audiomixer.sink_2 \
  audiomixer name=audiomixer ! audioconvert ! audioresample ! \
  audio/x-raw,rate=48000,channels=2,format=S16LE ! opusenc ! oggmux ! \
  filesink location=room_mix.ogg
```

## Manifest Structure

The manifest file (`manifest.json`) contains all metadata about the recording session:

```mermaid
classDiagram
    class AudioRecordingManifest {
        +string EgressID
        +string RoomID
        +string RoomName
        +string SessionID
        +AudioRecordingFormat[] Formats
        +int32 SampleRate
        +int32 ChannelCount
        +string Encryption
        +int64 StartedAt
        +int64 EndedAt
        +ParticipantRecordingInfo[] Participants
        +RoomMixInfo RoomMix
        +AudioRecordingStatus Status
        +string Error
    }

    class ParticipantRecordingInfo {
        +string ParticipantID
        +string ParticipantIdentity
        +string TrackID
        +int64 JoinedAt
        +int64 LeftAt
        +ClockSyncInfo ClockSync
        +AudioArtifact[] Artifacts
    }

    class ClockSyncInfo {
        +int64 ServerTimestamp
        +uint32 RTPClockBase
        +uint32 ClockRate
    }

    class AudioArtifact {
        +AudioRecordingFormat Format
        +string Filename
        +string StorageURI
        +int64 Size
        +int64 DurationMs
        +string SHA256
        +int64 UploadedAt
        +string PresignedURL
    }

    class RoomMixInfo {
        +AudioRecordingStatus Status
        +AudioArtifact[] Artifacts
        +int64 MergedAt
        +string Error
    }

    AudioRecordingManifest "1" --> "*" ParticipantRecordingInfo
    AudioRecordingManifest "0..1" --> "1" RoomMixInfo
    ParticipantRecordingInfo "0..1" --> "1" ClockSyncInfo
    ParticipantRecordingInfo "*" --> "*" AudioArtifact
    RoomMixInfo "*" --> "*" AudioArtifact
```

## File Structure

### Storage Path Structure

```
{room_name}/{session_id}/
├── manifest.json
├── participants/
│   ├── participant1_identity_participant1_20260125T120000.ogg
│   ├── participant2_identity_participant2_20260125T120000.ogg
│   └── participant3_identity_participant3_20260125T120000.ogg
└── room_mix_20260125T120000.ogg
```

## Error Handling & Retry Logic

```mermaid
flowchart TD
    Start[Job Starts] --> Process[Process Job]
    Process --> Check{Success?}
    
    Check -->|Yes| Complete[Mark Completed]
    Check -->|No| CheckRetries{Retries < 3?}
    
    CheckRetries -->|Yes| Increment[Increment Retry Count]
    Increment --> Requeue[Requeue Job]
    Requeue --> Wait[Wait for Next Poll]
    Wait --> Process
    
    CheckRetries -->|No| Fail[Mark as Failed]
    
    Complete --> End[End]
    Fail --> End
    
    style Check fill:#f9f,stroke:#333,stroke-width:2px
    style CheckRetries fill:#bbf,stroke:#333,stroke-width:2px
    style Fail fill:#fbb,stroke:#333,stroke-width:2px
    style Complete fill:#bfb,stroke:#333,stroke-width:2px
```

## Configuration

### Merge Worker Configuration

```yaml
# Command line flags
--worker-id: Unique worker identifier
--tmp-dir: Temporary directory for merge operations
--config: Path to egress config file
--config-body: Config YAML as string
```

### Audio Recording Configuration

```yaml
audio_recording:
  room_name: "my-room"
  session_id: "session-123"
  isolated_tracks: true  # Record each participant separately
  final_room_mix: true   # Enable offline merge
  formats:
    - ogg_opus
    - wav_pcm
  sample_rate: 48000
  encryption:
    mode: aes_256
    master_key: <base64-encoded-key>
  storage:
    s3:
      access_key: "..."
      secret_key: "..."
      region: "us-east-1"
      bucket: "my-bucket"
```

## Performance Considerations

1. **Memory Efficiency**: 
   - Recording uses 20ms ring buffer (vs 500ms jitter buffer)
   - Streaming encryption with 64KB chunks
   - Temporary files cleaned up after merge

2. **Alignment Accuracy**:
   - Target: <20ms drift over 60 minutes
   - Uses server timestamps as primary reference
   - RTP clock info for fine-tuning

3. **Scalability**:
   - Multiple workers can process jobs in parallel
   - Redis queue supports horizontal scaling
   - Jobs are stateless and can be retried

4. **Storage Optimization**:
   - Participant files uploaded immediately when participant leaves
   - Manifest updated incrementally
   - Merged files only created if `final_room_mix: true`

## Security Features

1. **Encryption**:
   - AES-256-GCM envelope encryption
   - Per-file DEK (Data Encryption Key)
   - Master Key (KEK) never stored with data
   - Support for S3 SSE, GCS CMEK

2. **Integrity**:
   - SHA256 checksums for all artifacts
   - Verified on upload and download

3. **Access Control**:
   - Storage credentials managed separately
   - Presigned URLs for temporary access

## Testing

The commit includes comprehensive tests:
- `pkg/merge/merge_test.go`: Merge worker tests
- `pkg/encryption/encryption_test.go`: Encryption tests

Run tests with:
```bash
go test ./pkg/merge/...
go test ./pkg/encryption/...
```

## Summary

This commit introduces a complete offline audio merging system that:

1. **Records** individual participant audio tracks with clock sync information
2. **Stores** encrypted audio files and metadata in object storage
3. **Queues** merge jobs in Redis when sessions end
4. **Processes** merge jobs asynchronously using worker processes
5. **Aligns** tracks using server timestamps and RTP clock information
6. **Merges** tracks using GStreamer's audiomixer
7. **Uploads** merged files and updates manifests

The system is designed for scalability, reliability, and accuracy, with support for encryption, retry logic, and horizontal scaling of workers.
