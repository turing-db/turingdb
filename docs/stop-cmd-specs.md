# Stop command specifications

Design specifications for the stop command. The general idea is that we may
want to launch multiple instances of TuringDB that would run in separate 
`turing-dir` directories, and listen on different ports. The stop command
should be bullet proof and allow:

- Stop all instances of TuringDB with `-all`
- Stop the instance that listens on a specific port with `-port`
- Stop the instance that is running in a specific directory with `-turing-dir`
- Use a timeout to wait for the process to stop gracefully with `-timeout`
- Force kill the process with `-force` if the timeout is exceeded

This feature also impacts the `start` command since it requires a lock file

`TuringDB` is allowed to run without listening on a port, in which case, the `port`
section of the lock file will be set to `-1`.

> [!WARNING]
> warning
> These specs show `C` style snippets, the actual implementation may
> use more modern C++ features.

## File Structure and Formats

### Lock File Location
- **Path**: `{turing-dir}/turingdb.lock`
- **Permissions**: `0644` (readable by all, writable by owner)

### Lock File Format
```
<pid>
<port>
<start_timestamp_epoch>
```

**Example**:
```
12345
6666
1738684800
```

**Parsing**:

- Read file line by line
- Line 1: PID (integer)
- Line 2: Port (integer)
- Line 3: Unix timestamp (integer, seconds since epoch)

---

## Start Command Implementation

### 1. Check if turing-dir is Locked

**Algorithm**:
```cpp
int lock_fd = open("{turing-dir}/turingdb.lock", O_CREAT | O_RDWR, 0644);
if (lock_fd < 0) {
    perror("Failed to open lock file");
    exit(1);
}

// Try to acquire exclusive lock (non-blocking)
if (flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
    if (errno == EWOULDBLOCK) {
        // Lock is held by another process
        // Read the lock file to get PID
        char buf[256];
        lseek(lock_fd, 0, SEEK_SET);
        ssize_t n = read(lock_fd, buf, sizeof(buf) - 1);
        buf[n] = '\0';
        
        // Parse first line to get PID
        int existing_pid = atoi(buf);
        
        fprintf(stderr, "Error: Another turingdb instance (PID %d) is using %s\n",
                existing_pid, turing_dir);
        close(lock_fd);
        exit(1);
    }
    perror("Failed to acquire lock");
    exit(1);
}

// Successfully locked - continue with startup
// IMPORTANT: Keep lock_fd open for the lifetime of the process
```

**Error Cases**:

- Lock file creation fails → Exit with error
- Lock is held by another process → Report PID and exit
- Lock file is corrupted/unreadable → Clean up and retry

### 2. Check if Port is Already in Use

This should already be handled by `TuringServer`.

```cpp
int sock = socket(AF_INET, SOCK_STREAM, 0);
int opt = 1;
setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

struct sockaddr_in addr = {};
addr.sin_family = AF_INET;
addr.sin_addr.s_addr = INADDR_ANY;
addr.sin_port = htons(6666);

if (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    if (errno == EADDRINUSE) {
        fprintf(stderr, "Error: Port 6666 is already in use\n");
        // Optionally: find which process using lsof
        exit(1);
    }
    perror("bind failed");
    exit(1);
}

// Successfully bound - keep socket open for server
```

### 3. Write Process Metadata to Lock File

**Algorithm**:
```cpp
// After successfully acquiring lock, write metadata
char hostname[256];
gethostname(hostname, sizeof(hostname));

char lock_content[512];
int len = snprintf(lock_content, sizeof(lock_content),
                   "%d\n%d\n%ld\n%s\n",
                   getpid(),
                   6666,  // port
                   (long)time(NULL),  // start timestamp
                   hostname);

// Write atomically: truncate first, then write
ftruncate(lock_fd, 0);
lseek(lock_fd, 0, SEEK_SET);
write(lock_fd, lock_content, len);
fsync(lock_fd);  // Ensure written to disk

// Keep lock_fd open - do NOT close it
// Store lock_fd as a global or member variable for cleanup
```

**Cleanup on Exit**:

```cpp
// Register cleanup handler
atexit(cleanup_lock_file);

void cleanup_lock_file() {
    if (lock_fd >= 0) {
        // Lock is automatically released on close
        close(lock_fd);
        // Optionally delete the lock file
        unlink("{turing-dir}/turingdb.lock");
    }
}
```

---

## Stop Command Implementation

### Data Structures

```cpp
struct LockFileData {
    pid_t pid;
    int port;
    time_t start_time;
    char hostname[256];
    bool valid;
};

struct StopOptions {
    const char* turing_dir;  // NULL if using -port or -all
    int port;                // -1 if not using -port
    bool stop_all;           // true if -all flag
    int timeout_seconds;     // 0 means no wait, -1 means wait forever
    bool force;              // send SIGKILL after timeout
};
```

### Base Behavior: Stop by turing-dir

#### Step 1: Read and Parse Lock File

```cpp
LockFileData read_lock_file(const char* turing_dir) {
    LockFileData data = {0};
    data.valid = false;
    
    char lock_path[PATH_MAX];
    snprintf(lock_path, sizeof(lock_path), "%s/turingdb.lock", turing_dir);
    
    FILE* fp = fopen(lock_path, "r");
    if (!fp) {
        // File doesn't exist
        return data;
    }
    
    // Parse line by line
    if (fscanf(fp, "%d\n", &data.pid) != 1) {
        fclose(fp);
        return data;
    }
    
    if (fscanf(fp, "%d\n", &data.port) != 1) {
        fclose(fp);
        return data;
    }
    
    long timestamp;
    if (fscanf(fp, "%ld\n", &timestamp) != 1) {
        fclose(fp);
        return data;
    }
    data.start_time = (time_t)timestamp;
    
    if (fgets(data.hostname, sizeof(data.hostname), fp) == NULL) {
        fclose(fp);
        return data;
    }
    
    // Remove trailing newline
    data.hostname[strcspn(data.hostname, "\n")] = '\0';
    
    fclose(fp);
    data.valid = true;
    return data;
}
```

#### Step 2: Validate PID (Cross-Platform)

```cpp
bool is_process_alive(pid_t pid) {
    // Works on both platforms
    if (kill(pid, 0) == 0) {
        return true;  // Process exists and we can signal it
    }
    
    if (errno == EPERM) {
        return true;  // Process exists but no permission
    }
    
    return false;  // Process doesn't exist (ESRCH)
}

bool is_turingdb_process(pid_t pid) {
#ifdef __linux__
    char path[256];
    snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);
    
    FILE* fp = fopen(path, "r");
    if (!fp) return false;
    
    char cmdline[1024];
    size_t n = fread(cmdline, 1, sizeof(cmdline) - 1, fp);
    cmdline[n] = '\0';
    fclose(fp);
    
    // cmdline has null-separated arguments
    // Check if it contains "turingdb"
    return strstr(cmdline, "turingdb") != NULL;
    
#elif __APPLE__
    char pathbuf[PROC_PIDPATHINFO_MAXSIZE];
    int ret = proc_pidpath(pid, pathbuf, sizeof(pathbuf));
    if (ret <= 0) return false;
    
    // Check if path contains "turingdb"
    return strstr(pathbuf, "turingdb") != NULL;
#endif
}
```

#### Step 3: Graceful Shutdown Sequence

```cpp
enum StopResult {
    STOP_SUCCESS,
    STOP_TIMEOUT,
    STOP_FORCE_KILLED,
    STOP_NOT_FOUND,
    STOP_ERROR
};

StopResult stop_process(pid_t pid, const StopOptions* opts) {
    // Send SIGTERM
    if (kill(pid, SIGTERM) != 0) {
        if (errno == ESRCH) {
            return STOP_NOT_FOUND;
        }
        perror("Failed to send SIGTERM");
        return STOP_ERROR;
    }
    
    printf("Sent SIGTERM to process %d\n", pid);
    
    // If no timeout, return immediately
    if (opts->timeout_seconds == 0) {
        return STOP_SUCCESS;
    }
    
    // Wait for process to exit
    int elapsed = 0;
    const int poll_interval_ms = 100;
    int max_iterations = (opts->timeout_seconds > 0) 
                        ? (opts->timeout_seconds * 1000 / poll_interval_ms)
                        : INT_MAX;  // Wait forever if timeout is -1
    
    for (int i = 0; i < max_iterations; i++) {
        if (!is_process_alive(pid)) {
            printf("Process %d stopped gracefully\n", pid);
            return STOP_SUCCESS;
        }
        
        // Sleep for poll_interval_ms milliseconds
        struct timespec ts = {
            .tv_sec = 0,
            .tv_nsec = poll_interval_ms * 1000000L
        };
        nanosleep(&ts, NULL);
        
        elapsed += poll_interval_ms;
        
        // Print progress every second
        if (elapsed % 1000 == 0) {
            printf("Waiting for process to stop... (%d seconds)\n", 
                   elapsed / 1000);
        }
    }
    
    // Timeout expired
    printf("Timeout expired after %d seconds\n", opts->timeout_seconds);
    
    if (opts->force) {
        printf("Sending SIGKILL to process %d\n", pid);
        if (kill(pid, SIGKILL) != 0) {
            perror("Failed to send SIGKILL");
            return STOP_ERROR;
        }
        
        // Wait a bit for SIGKILL to take effect
        sleep(1);
        
        if (!is_process_alive(pid)) {
            printf("Process %d force killed\n", pid);
            return STOP_FORCE_KILLED;
        }
        
        return STOP_ERROR;
    }
    
    return STOP_TIMEOUT;
}
```

#### Step 4: Main Stop Logic

```cpp
int stop_by_turing_dir(const StopOptions* opts) {
    LockFileData data = read_lock_file(opts->turing_dir);
    
    if (!data.valid) {
        fprintf(stderr, "No lock file found in %s\n", opts->turing_dir);
        fprintf(stderr, "No turingdb instance appears to be running\n");
        return 1;
    }
    
    printf("Found turingdb instance (PID %d, port %d)\n", 
           data.pid, data.port);
    
    // Validate PID
    if (!is_process_alive(data.pid)) {
        fprintf(stderr, "Process %d is not running (stale lock file)\n", 
                data.pid);
        
        // Clean up stale lock file
        char lock_path[PATH_MAX];
        snprintf(lock_path, sizeof(lock_path), 
                 "%s/turingdb.lock", opts->turing_dir);
        unlink(lock_path);
        
        return 1;
    }
    
    if (!is_turingdb_process(data.pid)) {
        fprintf(stderr, 
                "Process %d exists but is not a turingdb process\n", 
                data.pid);
        fprintf(stderr, "Lock file may be corrupted\n");
        return 1;
    }
    
    // Stop the process
    StopResult result = stop_process(data.pid, opts);
    
    // Clean up lock file if process stopped
    if (result == STOP_SUCCESS || result == STOP_FORCE_KILLED) {
        char lock_path[PATH_MAX];
        snprintf(lock_path, sizeof(lock_path), 
                 "%s/turingdb.lock", opts->turing_dir);
        unlink(lock_path);
    }
    
    return (result == STOP_SUCCESS || result == STOP_FORCE_KILLED) ? 0 : 1;
}
```

---

## Stop by Port (-port flag)

### Step 1: Find Process Using Port

```cpp
pid_t find_process_on_port(int port) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "lsof -ti tcp:%d 2>/dev/null", port);
    
    FILE* fp = popen(cmd, "r");
    if (!fp) {
        return -1;
    }
    
    pid_t pid = -1;
    if (fscanf(fp, "%d", &pid) != 1) {
        pclose(fp);
        return -1;
    }
    
    pclose(fp);
    return pid;
}
```

### Step 2: Main Stop Logic

```cpp
int stop_by_port(const StopOptions* opts) {
    pid_t pid = find_process_on_port(opts->port);
    
    if (pid < 0) {
        fprintf(stderr, "No process found listening on port %d\n", 
                opts->port);
        return 1;
    }
    
    printf("Found process %d on port %d\n", pid, opts->port);
    
    // Validate it's a turingdb process
    if (!is_turingdb_process(pid)) {
        fprintf(stderr, 
                "Process %d is not a turingdb process. Refusing to stop.\n", 
                pid);
        return 1;
    }
    
    // Stop the process
    StopResult result = stop_process(pid, opts);
    
    return (result == STOP_SUCCESS || result == STOP_FORCE_KILLED) ? 0 : 1;
}
```

---

## Stop All (-all flag)

### Step 1: Find All turingdb Processes

```cpp
#include <vector>

std::vector<pid_t> find_all_turingdb_processes() {
    std::vector<pid_t> pids;
    
    // Use pgrep (works on both Linux and macOS)
    FILE* fp = popen("pgrep -f turingdb 2>/dev/null", "r");
    if (!fp) {
        return pids;
    }
    
    pid_t pid;
    while (fscanf(fp, "%d", &pid) == 1) {
        // Double-check it's actually turingdb
        if (is_turingdb_process(pid)) {
            pids.push_back(pid);
        }
    }
    
    pclose(fp);
    return pids;
}
```

### Step 2: Stop All Processes

```cpp
int stop_all(const StopOptions* opts) {
    std::vector<pid_t> pids = find_all_turingdb_processes();
    
    if (pids.empty()) {
        printf("No turingdb processes found\n");
        return 0;
    }
    
    printf("Found %zu turingdb process(es)\n", pids.size());
    
    int success_count = 0;
    int fail_count = 0;
    
    for (pid_t pid : pids) {
        printf("\nStopping process %d...\n", pid);
        
        StopResult result = stop_process(pid, opts);
        
        if (result == STOP_SUCCESS || result == STOP_FORCE_KILLED) {
            success_count++;
        } else {
            fail_count++;
        }
    }
    
    printf("\nSummary: %d stopped, %d failed\n", 
           success_count, fail_count);
    
    // Clean up orphaned lock files
    cleanup_orphaned_lock_files();
    
    return (fail_count == 0) ? 0 : 1;
}
```

### Step 3: Clean Up Orphaned Lock Files

```cpp
void cleanup_orphaned_lock_files() {
    // This is optional but helpful
    // Find all turing-dir directories and check their lock files
    
    // For now, this can be skipped or implemented as:
    // 1. User specifies --turing-dir-pattern
    // 2. Find matching directories
    // 3. Check their lock files for stale PIDs
    // 4. Remove stale lock files
    
    // Simple implementation: do nothing
    // Lock files will be cleaned up when user tries to start again
}
```

---

## Error Handling

### Common Error Cases

```cpp
// Case 1: Permission denied
if (errno == EPERM) {
    fprintf(stderr, 
            "Permission denied. Try running with sudo?\n");
}

// Case 2: Process doesn't exist
if (errno == ESRCH) {
    fprintf(stderr, 
            "Process no longer exists. May have crashed or been killed.\n");
}

// Case 3: Lock file is corrupted
if (!data.valid) {
    fprintf(stderr, 
            "Lock file is corrupted or unreadable.\n");
    fprintf(stderr, 
            "You may need to manually check for running processes.\n");
}

// Case 4: Signal failed
if (kill(pid, SIGTERM) != 0) {
    perror("Failed to send signal");
    fprintf(stderr, 
            "Try: kill -TERM %d\n", pid);
}
```

---

## Platform-Specific Implementation

### Header for Platform Detection

```cpp
// platform.h
#ifndef PLATFORM_H
#define PLATFORM_H

#include <stdbool.h>
#include <sys/types.h>

#ifdef __APPLE__
#include <libproc.h>
#include <sys/sysctl.h>
#endif

#ifdef __linux__
#include <stdio.h>
#include <string.h>
#endif

bool is_process_alive(pid_t pid);
bool is_turingdb_process(pid_t pid);

#endif
```

### Implementation File

```cpp
// platform.c

#include "platform.h"
#include <signal.h>
#include <errno.h>

bool is_process_alive(pid_t pid) {
    if (kill(pid, 0) == 0) {
        return true;
    }
    return errno == EPERM;  // Exists but no permission
}

bool is_turingdb_process(pid_t pid) {
#ifdef __linux__
    char path[256];
    snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);
    
    FILE* fp = fopen(path, "r");
    if (!fp) return false;
    
    char cmdline[1024];
    size_t n = fread(cmdline, 1, sizeof(cmdline) - 1, fp);
    cmdline[n] = '\0';
    fclose(fp);
    
    return strstr(cmdline, "turingdb") != NULL;
    
#elif __APPLE__
    char pathbuf[PROC_PIDPATHINFO_MAXSIZE];
    int ret = proc_pidpath(pid, pathbuf, sizeof(pathbuf));
    if (ret <= 0) return false;
    
    return strstr(pathbuf, "turingdb") != NULL;
    
#else
    #error "Unsupported platform"
#endif
}
```

---

## Testing Scenarios

### Test 1: Basic Start/Stop
```bash
# Start
turingdb -turing-dir ./test-dir

# Verify lock file
cat ./test-dir/turingdb.lock

# Stop
turingdb stop -turing-dir ./test-dir

# Verify lock file removed
ls ./test-dir/turingdb.lock  # Should not exist
```

### Test 2: Stale Lock File
```bash
# Start
turingdb -turing-dir ./test-dir

# Kill with SIGKILL externally
kill -9 $(cat ./test-dir/turingdb.lock | head -1)

# Try to stop
turingdb stop -turing-dir ./test-dir
# Should detect stale lock and clean up
```

### Test 3: Double Start
```bash
# Start first instance
turingdb -turing-dir ./test-dir

# Try to start second instance
turingdb -turing-dir ./test-dir
# Should fail with "already locked" error
```

### Test 4: Stop with Timeout
```bash
# Start
turingdb -turing-dir ./test-dir

# Stop with timeout
turingdb stop -turing-dir ./test-dir -timeout 10
# Should wait up to 10 seconds
```

### Test 5: Force Kill
```bash
# Start (and make it ignore SIGTERM for testing)
turingdb -turing-dir ./test-dir

# Stop with force
turingdb stop -turing-dir ./test-dir -timeout 5 -force
# Should SIGKILL after 5 seconds
```

### Test 6: Stop by Port
```bash
# Start on port 6666
turingdb -turing-dir ./test-dir -port 6666

# Stop by port
turingdb stop -port 6666
```

### Test 7: Stop All
```bash
# Start multiple instances
turingdb -turing-dir ./dir1 -port 6666
turingdb -turing-dir ./dir2 -port 6667

# Stop all
turingdb stop -all
```

---

## Build Configuration

### Compiler Flags

```makefile
# Linux
CFLAGS_LINUX = -D__linux__ -std=c11

# macOS
CFLAGS_MACOS = -D__APPLE__ -std=c11 -framework IOKit
LDFLAGS_MACOS = -lproc

# Detect platform
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    CFLAGS += $(CFLAGS_LINUX)
endif
ifeq ($(UNAME_S),Darwin)
    CFLAGS += $(CFLAGS_MACOS)
    LDFLAGS += $(LDFLAGS_MACOS)
endif
```

### Required Headers

```cpp
#include <sys/file.h>      // flock()
#include <sys/types.h>     // pid_t
#include <signal.h>        // kill(), SIGTERM, SIGKILL
#include <errno.h>         // errno, EWOULDBLOCK, ESRCH, EPERM
#include <unistd.h>        // getpid(), gethostname(), unlink()
#include <fcntl.h>         // open(), O_CREAT, O_RDWR
#include <time.h>          // time()
#include <stdio.h>         // FILE, fopen(), fscanf(), popen()
#include <stdlib.h>        // atoi(), exit()
#include <string.h>        // strstr(), strcspn()
#include <limits.h>        // PATH_MAX, INT_MAX

// Platform-specific
#ifdef __APPLE__
#include <libproc.h>       // proc_pidpath()
#endif
```

---

## Implementation Checklist

### Start Command
- [ ] Implement lock file acquisition with `flock()`
- [ ] Write PID, port, timestamp, hostname to lock file
- [x] Check port availability before starting
- [ ] Keep lock file descriptor open during process lifetime
- [ ] Register cleanup handler to remove lock file on exit
- [ ] Handle double-start detection

### Stop Command (Base)
- [ ] Implement lock file reading and parsing
- [ ] Implement PID validation (cross-platform)
- [ ] Implement graceful shutdown with SIGTERM
- [ ] Implement timeout waiting logic with progress reporting
- [ ] Implement force kill with SIGKILL
- [ ] Clean up lock file after successful stop
- [ ] Handle stale lock file detection and cleanup

### Stop Command (-port)
- [ ] Implement port-to-PID lookup using `lsof` (or something else)
- [ ] Validate process is turingdb before stopping
- [ ] Apply graceful shutdown sequence

### Stop Command (-all)
- [ ] Implement process discovery using `pgrep` (or something else)
- [ ] Iterate and stop all found processes
- [ ] Report summary of stopped/failed processes

### Cross-Platform Support
- [ ] Create platform abstraction layer (`platform.h/c`)
- [ ] Implement Linux-specific process validation using `/proc`
- [ ] Implement macOS-specific process validation using `proc_pidpath()`
- [ ] Test on both Linux and macOS -> with regress tests
- [ ] Handle platform-specific edge cases

### Error Handling
- [ ] Handle permission errors (EPERM)
- [ ] Handle process not found errors (ESRCH)
- [ ] Handle corrupted lock files
- [ ] Handle signal delivery failures
- [ ] Provide helpful error messages

### Testing
- [ ] Test all scenarios listed in Testing Scenarios section
- [ ] Verify cleanup on normal exit
- [ ] Verify cleanup on abnormal exit (SIGKILL)
- [ ] Test concurrent start attempts
- [ ] Test timeout behavior
- [ ] Test force kill behavior
