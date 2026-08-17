#include "storage_module_plugin.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;

// Module package version, injected at build time from metadata.json.
#ifndef STORAGE_MODULE_VERSION
#define STORAGE_MODULE_VERSION "0.0.0-dev"
#endif

#define FETCH_MANIFEST_TIMEOUT_MS 30000

// ---------------------------------------------------------------------------
// Storage Module — libstorage C++ wrapper
//
// libstorage functions are asynchronous: a command is dispatched to a worker
// thread and the result arrives via a StorageCallback.  This file implements
// several callback context types (a strategy pattern) that handle results in
// different ways.  The type hierarchy is:
//
//   AsyncCallbackBase
//     │  Base for all fire-and-forget async contexts.  The dispatcher calls
//     │  handleResponse() and deletes the context on any non-PROGRESS code.
//     │
//     ├── SimpleEventCtx    – start/stop: emits a named event to the host.
//     ├── ConnectCtx        – connect: same as Simple, but also owns and
//     │                       frees the C-string peer-address array.
//     ├── UploadFileCtx     – file upload: throttled progress + done event.
//     ├── UploadChunkCtx    – single-chunk upload: emits progress event.
//     └── DownloadStreamCtx – dual-mode download:
//                             * file-mode  → write to path, emit byte-count
//                             * chunk-mode → emit base64-encoded data chunks
//
//   SyncCtx
//     Synchronous-wait pattern: the caller allocates a SyncCtx on the heap,
//     issues the command, then blocks on the condvar.  On callback arrival
//     the result is copied into the context and the condvar is signalled.
//     An "abandoned" flag handles the rare race where the caller times out
//     before the callback fires — in that case the callback itself deletes
//     the context instead of the caller.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// base64 encoding helper — needed to safely embed binary chunk data in JSON.
// nlohmann::json::dump() requires valid UTF-8; raw download chunks are
// arbitrary bytes and will throw type_error.316 without encoding.
// ---------------------------------------------------------------------------
static std::string base64Encode(const char* data, size_t len) {
    static const char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        const unsigned char b0 = static_cast<unsigned char>(data[i]);
        const unsigned char b1 = (i + 1 < len) ? static_cast<unsigned char>(data[i + 1]) : 0;
        const unsigned char b2 = (i + 2 < len) ? static_cast<unsigned char>(data[i + 2]) : 0;
        out += kTable[b0 >> 2];
        out += kTable[((b0 & 0x03) << 4) | (b1 >> 4)];
        out += (i + 1 < len) ? kTable[((b1 & 0x0F) << 2) | (b2 >> 6)] : '=';
        out += (i + 2 < len) ? kTable[b2 & 0x3F] : '=';
    }
    return out;
}

// ---------------------------------------------------------------------------
// Callback base — all context objects inherit from this.
// Only used for the async (event-emitting) dispatch path.
// ---------------------------------------------------------------------------

struct AsyncCallbackBase {
    virtual void handleResponse(int ret, const char* msg, size_t len) = 0;
    virtual ~AsyncCallbackBase() = default;
};

// Static callback for async contexts (start/stop/connect/upload progress/download).
// Ownership: each AsyncCallbackBase is heap-allocated and deleted here on non-PROGRESS.
// libstorage invokes this callback even if the request wasn't sent to the thread.
static void asyncCallback(int ret, const char* msg, size_t len, void* userData) {
    if (!userData) return;
    auto* base = static_cast<AsyncCallbackBase*>(userData);
    base->handleResponse(ret, msg, len);
    if (ret != RET_PROGRESS) {
        delete base;
    }
}

// ---------------------------------------------------------------------------
// SyncCtx — used for synchronous (blocking) libstorage calls.
//
// Lifetime rules:
//   - Allocated on the heap by the caller before issuing the command.
//   - Caller waits on the condvar, then checks ctx->received.
//   - If received == true before timeout: caller reads result and deletes ctx.
//   - If timeout fires before callback: caller marks ctx->abandoned = true
//     (under the same mutex) and does NOT delete; the callback will delete
//     when it eventually fires.
//
// This `abandoned` pattern prevents use-after-free if libstorage calls the
// callback after the waiting thread has timed out.
// ---------------------------------------------------------------------------

struct SyncCtx {
    std::mutex mtx;
    std::condition_variable ready;
    int resultCode = -1;
    std::string resultMsg;
    bool received = false;
    std::atomic<bool> abandoned{false};
    // Keeps the string argument alive across the (potentially async) C call.
    std::string lifetimeArg;

    SyncCtx() = default;
    SyncCtx(const SyncCtx&) = delete;
    SyncCtx& operator=(const SyncCtx&) = delete;
};

static void syncCallback(int ret, const char* msg, size_t len, void* userData) {
    if (!userData) return;
    auto* ctx = static_cast<SyncCtx*>(userData);
    bool shouldDelete;
    {
        std::unique_lock<std::mutex> lock(ctx->mtx);
        ctx->resultCode = ret;
        ctx->resultMsg = (msg && len > 0) ? std::string(msg, len) : std::string();
        ctx->received = true;
        ctx->ready.notify_all();
        // Read abandoned while holding the lock so there is no race with the
        // caller's timeout path that also sets this flag under the lock.
        shouldDelete = ctx->abandoned.load();
    }
    if (shouldDelete) {
        delete ctx;
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static constexpr int64_t DEFAULT_CHUNK_SIZE = 65536;
static constexpr int DOWNLOAD_PROTOCOL_VERSION = 2;
static constexpr int DOWNLOAD_CANCEL_TIMEOUT_MS = 15000;
#if defined(STORAGE_MODULE_TEST_CANCEL_WAIT_TIMEOUT_MS)
static constexpr int DOWNLOAD_CANCEL_WAIT_TIMEOUT_MS =
    STORAGE_MODULE_TEST_CANCEL_WAIT_TIMEOUT_MS;
#else
static constexpr int DOWNLOAD_CANCEL_WAIT_TIMEOUT_MS = DOWNLOAD_CANCEL_TIMEOUT_MS;
#endif
static constexpr int DOWNLOAD_CHUNK_TIMEOUT_MS = 60000;
static constexpr int MAX_DOWNLOAD_V2_BYTES = 1073741824;
static constexpr int MAX_DOWNLOAD_V2_CHUNK_BYTES = 1048576;
static constexpr size_t MAX_TERMINAL_DOWNLOADS_V2 = 128;
static std::atomic<uint64_t> nextDownloadV2StagingFileId{0};

static std::string fromMsg(const char* msg, size_t len) {
    return (msg && len > 0) ? std::string(msg, len) : std::string();
}

static constexpr std::uint8_t STORAGE_LIFECYCLE_NOT_INITIALIZED = 0;
static constexpr std::uint8_t STORAGE_LIFECYCLE_INITIALIZING = 1;
static constexpr std::uint8_t STORAGE_LIFECYCLE_STOPPED = 2;
static constexpr std::uint8_t STORAGE_LIFECYCLE_STARTING = 3;
static constexpr std::uint8_t STORAGE_LIFECYCLE_RUNNING = 4;
static constexpr std::uint8_t STORAGE_LIFECYCLE_STOPPING = 5;
static constexpr std::uint8_t STORAGE_LIFECYCLE_DESTROYING = 6;
static constexpr std::size_t MAX_NODE_LIFECYCLE_REQUEST_BYTES = 65536;
static constexpr std::size_t MAX_NODE_LIFECYCLE_CONFIG_BYTES = 49152;
static constexpr std::size_t MAX_NODE_LIFECYCLE_OPERATION_ID_BYTES = 128;
static constexpr std::size_t MAX_COMPLETED_NODE_LIFECYCLE_OPERATIONS = 128;
static constexpr const char* NODE_LIFECYCLE_SNAPSHOT_SCHEMA =
    "logos.managed_node_lifecycle.snapshot";
static constexpr const char* NODE_LIFECYCLE_COMMAND_SCHEMA =
    "logos.managed_node_lifecycle.command";
static constexpr const char* NODE_LIFECYCLE_ACK_SCHEMA =
    "logos.managed_node_lifecycle.ack";
static constexpr const char* NODE_LIFECYCLE_EVENT_SCHEMA =
    "logos.managed_node_lifecycle.event";
static std::atomic<std::uint64_t> nextNodeLifecycleInstanceSerial{0};

static const char* storageLifecycleStateName(std::uint8_t state) {
    switch (state) {
    case STORAGE_LIFECYCLE_NOT_INITIALIZED: return "not_initialized";
    case STORAGE_LIFECYCLE_INITIALIZING: return "not_initialized";
    case STORAGE_LIFECYCLE_STOPPED: return "stopped";
    case STORAGE_LIFECYCLE_STARTING: return "starting";
    case STORAGE_LIFECYCLE_RUNNING: return "running";
    case STORAGE_LIFECYCLE_STOPPING: return "stopping";
    case STORAGE_LIFECYCLE_DESTROYING: return "stopped";
    default: return "unknown";
    }
}

static const char* nodeLifecycleStateName(std::uint8_t state) {
    switch (state) {
    case STORAGE_LIFECYCLE_NOT_INITIALIZED: return "uninitialized";
    case STORAGE_LIFECYCLE_INITIALIZING: return "initializing";
    case STORAGE_LIFECYCLE_STOPPED: return "stopped";
    case STORAGE_LIFECYCLE_STARTING: return "starting";
    case STORAGE_LIFECYCLE_RUNNING: return "running";
    case STORAGE_LIFECYCLE_STOPPING: return "stopping";
    case STORAGE_LIFECYCLE_DESTROYING: return "destroying";
    default: return "uninitialized";
    }
}

static int64_t nodeLifecycleTimestampMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static std::string makeNodeLifecycleInstanceId() {
    const auto serial = nextNodeLifecycleInstanceSerial.fetch_add(1) + 1;
    const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return "storage-" + std::to_string(now) + "-" + std::to_string(serial);
}

static bool isValidNodeLifecycleOperationId(const std::string& operationId) {
    if (operationId.empty()
        || operationId.size() > MAX_NODE_LIFECYCLE_OPERATION_ID_BYTES) {
        return false;
    }
    return std::all_of(operationId.begin(), operationId.end(), [](unsigned char c) {
        return c >= 0x21 && c <= 0x7e;
    });
}

static std::vector<std::string> nodeLifecycleActions(std::uint8_t state) {
    switch (state) {
    case STORAGE_LIFECYCLE_NOT_INITIALIZED:
        return {"initialize"};
    case STORAGE_LIFECYCLE_STOPPED:
        return {"start", "destroy"};
    case STORAGE_LIFECYCLE_STARTING:
    case STORAGE_LIFECYCLE_RUNNING:
        return {"stop"};
    default:
        return {};
    }
}

static const char* lifecycleFailureMessage(const std::string& action) {
    if (action == "initialize") return "Storage initialization failed.";
    if (action == "start") return "Storage start failed.";
    if (action == "stop") return "Storage stop failed.";
    if (action == "destroy") return "Storage destruction failed.";
    return "Storage lifecycle action failed.";
}

static const char* lifecycleFailureCode(const std::string& action) {
    if (action == "initialize") return "initialize_failed";
    if (action == "start") return "start_failed";
    if (action == "stop") return "stop_failed";
    if (action == "destroy") return "destroy_failed";
    return "lifecycle_action_failed";
}

static json nodeLifecycleError(const std::string& code,
                               const std::string& message,
                               std::int64_t occurredAtMs) {
    return {
        {"code", code},
        {"message", message},
        {"occurred_at_ms", occurredAtMs},
    };
}

static bool containsEmbeddedNul(const std::string& value) {
    return value.find('\0') != std::string::npos;
}

static bool isValidDownloadV2OperationId(const std::string& operationId) {
    return !operationId.empty() && !containsEmbeddedNul(operationId)
        && operationId.find_first_of(" \t\r\n") == std::string::npos;
}

static bool manifestDatasetSize(const std::string& message, uint64_t& size) {
    try {
        const json manifest = json::parse(message);
        const auto value = manifest.find("datasetSize");
        if (value == manifest.end()) return false;
        if (value->is_number_unsigned()) {
            size = value->get<uint64_t>();
            return true;
        }
        if (value->is_number_integer()) {
            const int64_t signedSize = value->get<int64_t>();
            if (signedSize < 0) return false;
            size = static_cast<uint64_t>(signedSize);
            return true;
        }
    } catch (...) {
    }
    return false;
}

static fs::path downloadV2StagingPath(const std::string& destinationPath) {
    const fs::path destination(destinationPath);
    const auto timestamp = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const uint64_t serial = nextDownloadV2StagingFileId.fetch_add(1);
    return destination.parent_path()
        / (destination.filename().string() + ".storage-download-"
           + std::to_string(timestamp) + "-" + std::to_string(serial) + ".part");
}

static fs::path downloadV2BackupPath(const fs::path& destination) {
    const auto timestamp = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const uint64_t serial = nextDownloadV2StagingFileId.fetch_add(1);
    return destination.parent_path()
        / (destination.filename().string() + ".storage-download-"
           + std::to_string(timestamp) + "-" + std::to_string(serial) + ".backup");
}

// POSIX permits rename() to replace an existing file atomically, but Windows
// does not. Try the direct path first, then use a backup-and-restore fallback
// when an existing destination prevents replacement. On a failed fallback we
// restore the original destination or retain its backup for recovery before
// the caller removes the staging file.
static bool replaceDownloadV2Destination(const fs::path& stagingPath,
                                         const fs::path& destinationPath,
                                         std::string& error) {
    std::error_code destinationStatusError;
    const bool destinationExists = fs::exists(destinationPath, destinationStatusError);
    if (destinationStatusError) {
        error = "Failed to inspect download destination: "
            + destinationStatusError.message();
        return false;
    }
    if (destinationExists) {
        std::error_code destinationTypeError;
        const bool destinationIsRegular =
            fs::is_regular_file(destinationPath, destinationTypeError);
        if (destinationTypeError || !destinationIsRegular) {
            error = destinationTypeError
                ? "Failed to inspect download destination: " + destinationTypeError.message()
                : "Failed to replace download destination: destination is not a regular file.";
            return false;
        }
    }

    std::error_code renameError;
    fs::rename(stagingPath, destinationPath, renameError);
    if (!renameError) return true;

    std::error_code existsError;
    const bool destinationStillExists = fs::exists(destinationPath, existsError);
    if (existsError || !destinationStillExists) {
        error = "Failed to replace download destination: " + renameError.message();
        return false;
    }

    std::error_code destinationTypeError;
    const bool destinationIsRegular =
        fs::is_regular_file(destinationPath, destinationTypeError);
    if (destinationTypeError || !destinationIsRegular) {
        error = destinationTypeError
            ? "Failed to inspect download destination: " + destinationTypeError.message()
            : "Failed to replace download destination: destination is not a regular file.";
        return false;
    }

    const fs::path backupPath = downloadV2BackupPath(destinationPath);
    std::error_code backupError;
    fs::rename(destinationPath, backupPath, backupError);
    if (backupError) {
        error = "Failed to replace download destination: " + backupError.message();
        return false;
    }

    std::error_code replacementError;
    fs::rename(stagingPath, destinationPath, replacementError);
    if (!replacementError) {
        // Do not remove the original until the replacement is in place. If
        // cleanup itself fails, retain the backup rather than risk data loss.
        std::error_code cleanupError;
        fs::remove(backupPath, cleanupError);
        return true;
    }

    std::error_code restoreError;
    fs::rename(backupPath, destinationPath, restoreError);
    error = "Failed to replace download destination: " + replacementError.message();
    if (restoreError) {
        error += " Failed to restore original destination: " + restoreError.message()
            + ". Original destination retained at " + backupPath.string() + ".";
    }
    return false;
}

enum class DownloadOwner {
    Legacy,
    Versioned,
};

enum class DownloadPhase {
    Initializing,
    Active,
    AwaitingLateInit,
    Cleaning,
};

struct DownloadLease {
    std::string cid;
    DownloadOwner owner;
    std::string operationId;
    DownloadPhase phase = DownloadPhase::Initializing;
};

struct DownloadRegistry;

// A V2 transfer uses one libstorage chunk request at a time. This keeps the
// storage worker free between chunks, which is necessary because libstorage
// cannot cancel a stream-mode download while its worker is busy.
struct DownloadV2State {
    std::string cid;
    std::string operationId;
    std::shared_ptr<DownloadRegistry> registry;
    std::shared_ptr<DownloadLease> lease;
    std::mutex mutex;
    std::condition_variable initializationReady;
    std::condition_variable cancellationReady;
    bool initializationResolved = false;
    bool initializationSucceeded = false;
    std::string initializationError;
    bool dispatchResolved = false;
    bool dispatchAccepted = false;
    bool shutdownRequested = false;
    bool cancellationRequested = false;
    bool cancellationBeforeInitialization = false;
    bool cancellationDispatched = false;
    bool cancellationResolved = false;
    bool cancellationSucceeded = false;
    std::string cancellationError;
    bool releaseLeaseOnCancellationConfirmation = false;
    std::atomic<bool> workerFinished{false};
};

struct TerminalDownloadV2 {
    std::string cid;
    std::string outcome;
};

struct DownloadLeaseCleanupWorker {
    std::shared_ptr<std::atomic<bool>> finished;
    std::thread thread;
};

struct DownloadRegistry {
    std::mutex mutex;
    bool closing = false;
    std::unordered_map<std::string, std::shared_ptr<DownloadLease>> leasesByCid;
    std::unordered_set<std::string> pendingOperationIdsV2;
    std::unordered_map<std::string, std::shared_ptr<DownloadV2State>> activeDownloadsV2;
    std::unordered_map<std::string, TerminalDownloadV2> terminalDownloadsV2;
    std::deque<std::string> terminalDownloadOrderV2;
    std::vector<DownloadLeaseCleanupWorker> cleanupWorkers;
};

static std::shared_ptr<DownloadLease> reserveDownloadLease(
    const std::shared_ptr<DownloadRegistry>& registry, const std::string& cid,
    DownloadOwner owner, const std::string& operationId, std::string& error) {
    std::lock_guard<std::mutex> lock(registry->mutex);
    if (registry->closing) {
        error = "Storage context is shutting down.";
        return {};
    }
    const auto existing = registry->leasesByCid.find(cid);
    if (existing != registry->leasesByCid.end()) {
        error = existing->second->phase == DownloadPhase::Initializing
                || existing->second->phase == DownloadPhase::AwaitingLateInit
            ? "A download for this CID is already starting."
            : "A download for this CID is already active.";
        return {};
    }
    if (owner == DownloadOwner::Versioned
        && (registry->pendingOperationIdsV2.find(operationId)
                != registry->pendingOperationIdsV2.end()
            || registry->activeDownloadsV2.find(operationId)
                != registry->activeDownloadsV2.end()
            || registry->terminalDownloadsV2.find(operationId)
                != registry->terminalDownloadsV2.end())) {
        error = "Download operation ID is already in use.";
        return {};
    }

    auto lease = std::make_shared<DownloadLease>(
        DownloadLease{cid, owner, operationId, DownloadPhase::Initializing});
    registry->leasesByCid.emplace(cid, lease);
    if (owner == DownloadOwner::Versioned) {
        registry->pendingOperationIdsV2.insert(operationId);
    }
    return lease;
}

static void releaseDownloadLease(const std::shared_ptr<DownloadRegistry>& registry,
                                 const std::shared_ptr<DownloadLease>& lease) {
    if (!registry || !lease) return;
    std::lock_guard<std::mutex> lock(registry->mutex);
    const auto current = registry->leasesByCid.find(lease->cid);
    if (current != registry->leasesByCid.end() && current->second == lease) {
        registry->leasesByCid.erase(current);
    }
    if (lease->owner == DownloadOwner::Versioned) {
        registry->pendingOperationIdsV2.erase(lease->operationId);
    }
}

static void setDownloadLeasePhase(const std::shared_ptr<DownloadRegistry>& registry,
                                  const std::shared_ptr<DownloadLease>& lease,
                                  DownloadPhase phase) {
    if (!registry || !lease) return;
    std::lock_guard<std::mutex> lock(registry->mutex);
    const auto current = registry->leasesByCid.find(lease->cid);
    if (current != registry->leasesByCid.end() && current->second == lease) {
        lease->phase = phase;
    }
}

static bool isDownloadRegistryClosing(const std::shared_ptr<DownloadRegistry>& registry) {
    std::lock_guard<std::mutex> lock(registry->mutex);
    return registry->closing;
}

static void closeDownloadRegistry(const std::shared_ptr<DownloadRegistry>& registry) {
    std::lock_guard<std::mutex> lock(registry->mutex);
    registry->closing = true;
}

static void reapFinishedDownloadLeaseCleanupWorkers(
    const std::shared_ptr<DownloadRegistry>& registry) {
    std::vector<std::thread> finished;
    {
        std::lock_guard<std::mutex> lock(registry->mutex);
        auto worker = registry->cleanupWorkers.begin();
        while (worker != registry->cleanupWorkers.end()) {
            if (worker->finished->load(std::memory_order_acquire)) {
                finished.push_back(std::move(worker->thread));
                worker = registry->cleanupWorkers.erase(worker);
            } else {
                ++worker;
            }
        }
    }
    for (std::thread& worker : finished) {
        if (worker.joinable()) worker.join();
    }
}

static bool activateDownloadV2(const std::shared_ptr<DownloadV2State>& state) {
    const auto& registry = state->registry;
    std::lock_guard<std::mutex> lock(registry->mutex);
    if (registry->closing) return false;
    registry->pendingOperationIdsV2.erase(state->operationId);
    registry->activeDownloadsV2[state->operationId] = state;
    return true;
}

static void abandonDownloadV2Start(const std::shared_ptr<DownloadV2State>& state) {
    const auto& registry = state->registry;
    {
        std::lock_guard<std::mutex> lock(registry->mutex);
        const auto active = registry->activeDownloadsV2.find(state->operationId);
        if (active != registry->activeDownloadsV2.end() && active->second == state) {
            registry->activeDownloadsV2.erase(active);
        }
    }
    releaseDownloadLease(registry, state->lease);
    state->workerFinished.store(true);
}

struct SyncResult {
    bool ok = false;
    std::string message;
};

static void enqueueDownloadLeaseCleanup(
    void* storageCtx, const std::shared_ptr<DownloadRegistry>& registry,
    const std::shared_ptr<DownloadLease>& lease);

// Wait for a SyncCtx to be signalled (or time out).
// Returns the result and handles the abandoned-flag cleanup.
static SyncResult waitSync(SyncCtx* ctx, int timeoutMs) {
    SyncResult r;
    bool shouldDelete;
    {
        std::unique_lock<std::mutex> lock(ctx->mtx);
        ctx->ready.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                         [ctx] { return ctx->received; });
        r.ok = ctx->received && ctx->resultCode == RET_OK;
        r.message = ctx->resultMsg;
        shouldDelete = ctx->received;
        if (!shouldDelete) {
            ctx->abandoned.store(true);
        }
    }
    if (shouldDelete) {
        delete ctx;
    }
    return r;
}

// ---------------------------------------------------------------------------
// JSON event helpers — build and emit common response patterns.
//
// Each helper takes a pointer-to-member to the typed event method declared
// in `storage_module_plugin.h`'s `logos_events:` block; the method bodies
// are codegen-emitted in `storage_module_events_cdylib.cpp` and dispatch
// through `LogosModuleContext::emitEventImpl_`.
//
// Serialization of the JSON payload runs inside libstorage callbacks where
// uncaught exceptions would be fatal; do that first under a try/catch and
// only invoke the typed event method when we have a valid string.
// ---------------------------------------------------------------------------

using StorageEvent = void (StorageModuleImpl::*)(const std::string&);

static void emitJsonEvent(StorageModuleImpl* impl, StorageEvent emit,
                          const json& payload, const char* caller) {
    std::string data;
    try {
        data = payload.dump();
    } catch (const std::exception& e) {
        fprintf(stderr, "%s: failed to serialize event payload: %s\n", caller, e.what());
        return;
    } catch (...) {
        fprintf(stderr, "%s: failed to serialize event payload (unknown error)\n", caller);
        return;
    }
    (impl->*emit)(data);
}

static void emitBasicResponse(StorageModuleImpl* impl, StorageEvent emit,
                              int ret, const std::string& message, const char* caller) {
    json j;
    j["success"] = (ret == RET_OK);
    j["message"] = message;
    emitJsonEvent(impl, emit, j, caller);
}

static void emitSessionProgress(StorageModuleImpl* impl, StorageEvent emit,
                                const std::string& sessionId, int64_t bytes,
                                const char* caller) {
    json j;
    j["success"] = true;
    j["sessionId"] = sessionId;
    j["bytes"] = bytes;
    emitJsonEvent(impl, emit, j, caller);
}

static void emitSessionResult(StorageModuleImpl* impl, StorageEvent emit,
                              int ret, const std::string& sessionId,
                              const std::string& message,
                              const std::string& okField, const char* caller) {
    json j;
    j["success"] = (ret == RET_OK);
    j["sessionId"] = sessionId;
    if (ret == RET_OK && !okField.empty()) j[okField] = message;
    else if (ret != RET_OK) j["error"] = message;
    emitJsonEvent(impl, emit, j, caller);
}

// ---------------------------------------------------------------------------
// Concrete async context implementations
// ---------------------------------------------------------------------------

// Dispatches the typed event member pointer passed in `event` on completion.
// JSON payload: {success, message}.
struct SimpleEventCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    StorageEvent event;
    std::atomic<std::uint64_t>* lifecycleGeneration;
    std::uint64_t generation;
    std::string lifecycleAction;
    std::string operationId;
    std::uint8_t previousState;
    std::uint8_t successState;
    std::uint8_t failureState;

    SimpleEventCtx(StorageModuleImpl* i, StorageEvent ev,
                   std::atomic<std::uint64_t>* generationCounter = nullptr,
                   std::uint64_t operationGeneration = 0,
                   std::string action = {}, std::string operation = {},
                   std::uint8_t priorState = STORAGE_LIFECYCLE_NOT_INITIALIZED,
                   std::uint8_t completedState = STORAGE_LIFECYCLE_NOT_INITIALIZED,
                   std::uint8_t failedState = STORAGE_LIFECYCLE_NOT_INITIALIZED)
        : impl(i), event(ev), lifecycleGeneration(generationCounter),
          generation(operationGeneration), lifecycleAction(std::move(action)),
          operationId(std::move(operation)), previousState(priorState),
          successState(completedState), failureState(failedState) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        if (impl && lifecycleGeneration
            && lifecycleGeneration->load() == generation) {
            impl->settleLifecycleAction(lifecycleAction, operationId,
                                        generation, previousState,
                                        successState, failureState,
                                        ret == RET_OK);
        }
        emitBasicResponse(impl, event, ret, fromMsg(msg, len), "SimpleEventCtx");
    }
};

// Same as SimpleEventCtx but owns the C-string peer-address array allocated
// by the caller and frees it on destruction.
// JSON payload: {success, message}.
struct ConnectCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string peerIdBuf;
    std::vector<char*> addrs;

    ConnectCtx(StorageModuleImpl* i, std::string pid, std::vector<char*> a)
        : impl(i), peerIdBuf(std::move(pid)), addrs(std::move(a)) {}

    ~ConnectCtx() override {
        for (char* p : addrs) free(p);
    }

    void handleResponse(int ret, const char* msg, size_t len) override {
        emitBasicResponse(impl, &StorageModuleImpl::storageConnect, ret,
                          fromMsg(msg, len), "ConnectCtx");
    }
};

// Handles file upload callbacks.
//
// On RET_PROGRESS: accumulates bytes and emits "storageUploadProgress" events
// throttled to at most one per percentage point (max 100 events total) to avoid
// flooding the caller.  When totalBytes is 0 (unknown), every event is forwarded.
// JSON payload: {success:true, sessionId, bytes}
//
// On RET_OK / error: emits "storageUploadDone".
// JSON payload: {success, sessionId, cid} on success; {success:false, sessionId, error} on failure.
struct UploadFileCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string sessionId;
    int64_t totalBytes;
    mutable int64_t bytesUploaded = 0;
    mutable int64_t pendingBytes = 0;
    mutable int lastEmittedPercent = -1;

    UploadFileCtx(StorageModuleImpl* i, std::string sid, int64_t total)
        : impl(i), sessionId(std::move(sid)), totalBytes(total) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        if (ret == RET_PROGRESS) {
            bytesUploaded += static_cast<int64_t>(len);
            pendingBytes  += static_cast<int64_t>(len);
            if (totalBytes > 0) {
                int percent =
                    static_cast<int>((bytesUploaded * 100LL) / totalBytes);
                if (percent <= lastEmittedPercent) return;
                lastEmittedPercent = percent;
            }
            emitSessionProgress(impl, &StorageModuleImpl::storageUploadProgress,
                                sessionId, pendingBytes, "UploadFileCtx");
            pendingBytes = 0;
            return;
        }
        emitSessionResult(impl, &StorageModuleImpl::storageUploadDone, ret,
                          sessionId, fromMsg(msg, len), "cid", "UploadFileCtx");
    }
};

// Handles a single manual chunk upload.
// Emits "storageUploadProgress" on completion.
// JSON payload on success:  {success:true,  sessionId, bytes}
// JSON payload on failure:  {success:false, sessionId, error}
//
// Note: we do NOT cancel the upload session on failure — a failed chunk does
// not corrupt the session, and the caller may choose to retry or abort.
struct UploadChunkCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string sessionId;
    std::string chunk;

    UploadChunkCtx(StorageModuleImpl* i, std::string sid, std::string c)
        : impl(i), sessionId(std::move(sid)), chunk(std::move(c)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        json j;
        j["success"] = (ret == RET_OK);
        j["sessionId"] = sessionId;
        if (ret == RET_OK) j["bytes"] = static_cast<int64_t>(chunk.size());
        else j["error"] = fromMsg(msg, len);
        emitJsonEvent(impl, &StorageModuleImpl::storageUploadProgress, j,
                      "UploadChunkCtx");
    }
};

// Handles streaming download callbacks.  Operates in two modes depending on
// whether filepath is empty:
//
//   Chunk mode (filepath empty):
//     On RET_PROGRESS: emits "storageDownloadProgress" with the received data
//     base64-encoded in the "chunk" field.  The data MUST be copied and encoded
//     here — the msg pointer is only valid for the duration of this call.
//     Base64 encoding is required because raw download data is arbitrary bytes
//     and nlohmann::json::dump() will throw on invalid UTF-8 sequences.
//     JSON payload: {success:true, sessionId, chunk:<base64>}
//
//   File mode (filepath non-empty):
//     On RET_PROGRESS: accumulates bytes and emits "storageDownloadProgress"
//     throttled to at most one event per percentage point to avoid flooding.
//     JSON payload: {success:true, sessionId, bytes}
//
//   Both modes on completion:
//     Emits "storageDownloadDone".
//     JSON payload on success:  {success:true,  sessionId}
//     JSON payload on failure:  {success:false, sessionId, error}
struct DownloadStreamCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string cid;
    std::string filepath;
    std::shared_ptr<DownloadRegistry> registry;
    std::shared_ptr<DownloadLease> lease;
    std::atomic<unsigned int> references{2};
    std::mutex dispatchMutex;
    bool dispatchResolved = false;
    bool dispatchAccepted = false;
    bool terminalReceived = false;
    int64_t totalBytes;
    mutable int64_t bytesDownloaded = 0;
    mutable int64_t pendingBytes = 0;
    mutable int lastEmittedPercent = -1;

    DownloadStreamCtx(StorageModuleImpl* i, std::string c, std::string fp,
                      std::shared_ptr<DownloadRegistry> r,
                      std::shared_ptr<DownloadLease> l, int64_t total = 0)
        : impl(i), cid(std::move(c)), filepath(std::move(fp)), registry(std::move(r)),
          lease(std::move(l)), totalBytes(total) {}

    void releaseReference() {
        if (references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

    void handleResponse(int ret, const char* msg, size_t len) override {
        if (ret == RET_PROGRESS) {
            if (filepath.empty()) {
                // Chunk mode — base64-encode the raw bytes so they can be
                // safely embedded in JSON.  The pointer is only valid here.
                json j;
                j["success"] = true;
                j["sessionId"] = cid;
                j["chunk"] = base64Encode(msg, len);
                emitJsonEvent(impl, &StorageModuleImpl::storageDownloadProgress,
                              j, "DownloadStreamCtx");
            } else {
                // File mode — report byte count, throttled to one event per
                // percentage point so large files don't flood the caller.
                bytesDownloaded += static_cast<int64_t>(len);
                pendingBytes    += static_cast<int64_t>(len);
                if (totalBytes > 0) {
                    int percent = static_cast<int>(
                        (bytesDownloaded * 100LL) / totalBytes);
                    if (percent <= lastEmittedPercent) return;
                    lastEmittedPercent = percent;
                }
                emitSessionProgress(impl,
                                    &StorageModuleImpl::storageDownloadProgress,
                                    cid, pendingBytes, "DownloadStreamCtx");
                pendingBytes = 0;
            }
            return;
        }
        emitSessionResult(impl, &StorageModuleImpl::storageDownloadDone, ret,
                          cid, fromMsg(msg, len), "", "DownloadStreamCtx");
    }
};

static void legacyDownloadStreamCallback(int ret, const char* msg, size_t len,
                                         void* userData) {
    if (!userData) return;
    auto* ctx = static_cast<DownloadStreamCtx*>(userData);
    ctx->handleResponse(ret, msg, len);
    if (ret == RET_PROGRESS) return;

    bool releaseLease = false;
    {
        std::lock_guard<std::mutex> lock(ctx->dispatchMutex);
        ctx->terminalReceived = true;
        releaseLease = ctx->dispatchResolved && ctx->dispatchAccepted;
    }
    if (releaseLease) releaseDownloadLease(ctx->registry, ctx->lease);
    ctx->releaseReference();
}

struct DownloadV2CancelCtx {
    std::shared_ptr<DownloadV2State> state;
};

static void downloadV2CancelCallback(int ret, const char* msg, size_t len,
                                     void* userData) {
    if (!userData || ret == RET_PROGRESS) return;
    auto* ctx = static_cast<DownloadV2CancelCtx*>(userData);
    bool releaseLease = false;
    {
        std::lock_guard<std::mutex> lock(ctx->state->mutex);
        ctx->state->cancellationResolved = true;
        ctx->state->cancellationSucceeded = ret == RET_OK;
        ctx->state->cancellationError = ret == RET_OK
            ? std::string()
            : fromMsg(msg, len);
        if (ret == RET_OK
            && ctx->state->releaseLeaseOnCancellationConfirmation) {
            ctx->state->releaseLeaseOnCancellationConfirmation = false;
            releaseLease = true;
        }
    }
    ctx->state->cancellationReady.notify_all();
    if (releaseLease) {
        releaseDownloadLease(ctx->state->registry, ctx->state->lease);
    }
    delete ctx;
}

static bool requestDownloadV2Cancellation(
    void* storageCtx, const std::shared_ptr<DownloadV2State>& state) {
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->cancellationDispatched) return true;
        state->cancellationDispatched = true;
    }

    auto resolveDispatchFailure = [&state](const std::string& error) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->cancellationResolved = true;
            state->cancellationSucceeded = false;
            state->cancellationError = error;
        }
        state->cancellationReady.notify_all();
    };

    if (!storageCtx) {
        resolveDispatchFailure("Storage context not initialized.");
        return false;
    }

    auto* ctx = new DownloadV2CancelCtx{state};
    if (storage_download_cancel(storageCtx, state->cid.c_str(),
                                downloadV2CancelCallback, ctx) != RET_OK) {
        // libstorage invokes the error callback synchronously before returning
        // RET_ERR. The callback owns and deletes ctx in both success and
        // failure cases, so deleting it here would double-free the context.
        return false;
    }
    return true;
}

static SyncResult waitForDownloadV2Cancellation(
    const std::shared_ptr<DownloadV2State>& state) {
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->cancellationReady.wait_for(
            lock, std::chrono::milliseconds(DOWNLOAD_CANCEL_WAIT_TIMEOUT_MS),
            [&state] { return state->cancellationResolved; })) {
        return {false, "Timed out waiting for download cancellation."};
    }
    if (!state->cancellationSucceeded) {
        return {false, state->cancellationError.empty()
                ? "Storage download cancellation failed."
                : state->cancellationError};
    }
    return {true, {}};
}

struct DownloadV2InitCtx {
    std::shared_ptr<DownloadV2State> state;
    void* storageCtx;
};

static void downloadV2InitCallback(int ret, const char* msg, size_t len,
                                   void* userData) {
    if (!userData || ret == RET_PROGRESS) return;
    auto* ctx = static_cast<DownloadV2InitCtx*>(userData);
    bool canceledBeforeInitialization = false;
    {
        std::lock_guard<std::mutex> lock(ctx->state->mutex);
        ctx->state->initializationResolved = true;
        ctx->state->initializationSucceeded = ret == RET_OK;
        ctx->state->initializationError = ret == RET_OK
            ? std::string()
            : fromMsg(msg, len);
        canceledBeforeInitialization = ctx->state->cancellationBeforeInitialization;
    }
    ctx->state->initializationReady.notify_all();
    if (canceledBeforeInitialization) {
        if (ret == RET_OK) {
            enqueueDownloadLeaseCleanup(ctx->storageCtx, ctx->state->registry,
                                        ctx->state->lease);
        } else {
            releaseDownloadLease(ctx->state->registry, ctx->state->lease);
        }
    }
    delete ctx;
}

struct DownloadV2ChunkCtx {
    explicit DownloadV2ChunkCtx(size_t limit) : maxBytes(limit) {}

    std::mutex mutex;
    std::condition_variable ready;
    std::vector<uint8_t> bytes;
    size_t maxBytes;
    int resultCode = RET_ERR;
    std::string resultMessage;
    bool completed = false;
    bool exceededLimit = false;
    std::atomic<bool> abandoned{false};
};

static void downloadV2ChunkCallback(int ret, const char* msg, size_t len,
                                    void* userData) {
    if (!userData) return;
    auto* ctx = static_cast<DownloadV2ChunkCtx*>(userData);
    if (ret == RET_PROGRESS) {
        std::lock_guard<std::mutex> lock(ctx->mutex);
        if (ctx->completed || ctx->exceededLimit) return;
        if ((!msg && len > 0) || len > ctx->maxBytes - ctx->bytes.size()) {
            ctx->exceededLimit = true;
            return;
        }
        if (len > 0) {
            const auto* bytes = reinterpret_cast<const uint8_t*>(msg);
            ctx->bytes.insert(ctx->bytes.end(), bytes, bytes + len);
        }
        return;
    }

    bool shouldDelete;
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);
        ctx->resultCode = ret;
        ctx->resultMessage = fromMsg(msg, len);
        ctx->completed = true;
        ctx->ready.notify_all();
        shouldDelete = ctx->abandoned.load();
    }
    if (shouldDelete) delete ctx;
}

struct DownloadV2ChunkResult {
    bool dispatched = false;
    bool completed = false;
    bool succeeded = false;
    bool exceededLimit = false;
    std::vector<uint8_t> bytes;
    std::string error;
};

static DownloadV2ChunkResult requestDownloadV2Chunk(void* storageCtx,
                                                     const std::string& cid,
                                                     size_t maxBytes) {
    DownloadV2ChunkResult result;
    if (!storageCtx) {
        result.error = "Storage context not initialized.";
        return result;
    }

    auto* ctx = new DownloadV2ChunkCtx(maxBytes);
    if (storage_download_chunk(storageCtx, cid.c_str(), downloadV2ChunkCallback,
                               ctx) != RET_OK) {
        delete ctx;
        result.error = "Failed to request download chunk.";
        return result;
    }
    result.dispatched = true;

    bool shouldDelete;
    {
        std::unique_lock<std::mutex> lock(ctx->mutex);
        if (!ctx->ready.wait_for(lock, std::chrono::milliseconds(DOWNLOAD_CHUNK_TIMEOUT_MS),
                                 [ctx] { return ctx->completed; })) {
            ctx->abandoned.store(true);
            result.error = "Timed out waiting for download chunk.";
            return result;
        }
        result.completed = true;
        result.succeeded = ctx->resultCode == RET_OK;
        result.exceededLimit = ctx->exceededLimit;
        result.bytes = std::move(ctx->bytes);
        result.error = ctx->resultMessage;
        shouldDelete = true;
    }
    if (shouldDelete) delete ctx;
    return result;
}

// Handles a background manifest fetch.  The DHT lookup can take a
// long time so it uses async callbacks to avoid blocking.
//
// On completion emits "storageDownloadManifestDone".
// JSON payload on success:  {success:true,  cid, manifest:{…}}
// JSON payload on failure:  {success:false, cid, error}
struct FetchManifestCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string cid;

    FetchManifestCtx(StorageModuleImpl* i, std::string c)
        : impl(i), cid(std::move(c)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        json j;
        j["cid"] = cid;
        if (ret == RET_OK) {
            try {
                j["manifest"] = json::parse(fromMsg(msg, len));
                j["success"] = true;
            } catch (...) {
                j["success"] = false;
                j["error"] = "Failed to parse manifest.";
            }
        } else {
            j["success"] = false;
            j["error"] = fromMsg(msg, len);
        }
        emitJsonEvent(impl, &StorageModuleImpl::storageDownloadManifestDone, j,
                      "FetchManifestCtx");
    }
};

// Handles a background content removal.  The delete may touch the network
// and can take a while, so it uses async callbacks to avoid blocking.
//
// On completion emits "storageRemoveDone".
// JSON payload on success:  {success:true,  cid}
// JSON payload on failure:  {success:false, cid, error}
struct RemoveCtx : AsyncCallbackBase {
    StorageModuleImpl* impl;
    std::string cid;

    RemoveCtx(StorageModuleImpl* i, std::string c)
        : impl(i), cid(std::move(c)) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
        json j;
        j["cid"] = cid;
        j["success"] = (ret == RET_OK);
        if (ret != RET_OK) {
            j["error"] = fromMsg(msg, len);
        }
        emitJsonEvent(impl, &StorageModuleImpl::storageRemoveDone, j,
                      "RemoveCtx");
    }
};

// ---------------------------------------------------------------------------
// syncCall wrappers — shorthand for the synchronous wait pattern.
//
// Each variant:
//   1. Allocates a SyncCtx on the heap.
//   2. Stores any string argument in ctx->lifetimeArg to keep it alive for
//      the duration of the async C call.
//   3. Issues the libstorage command; on immediate failure, deletes ctx and
//      returns an error.
//   4. Calls waitSync() to block until the callback fires or timeout expires.
// ---------------------------------------------------------------------------

using StorageNoArgFn = int (*)(void*, StorageCallback, void*);
using StorageBoolFn = int (*)(void*, bool, StorageCallback, void*);
using StorageStringFn = int (*)(void*, const char*, StorageCallback, void*);
using StorageStringIntFn = int (*)(void*, const char*, size_t, StorageCallback, void*);

static SyncResult syncCallNoArg(void* ctx, StorageNoArgFn fn, int timeoutMs) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    if (fn(ctx, syncCallback, sctx) != RET_OK) {
        delete sctx;
        return {false, "Failed to send command."};
    }
    return waitSync(sctx, timeoutMs);
}

static SyncResult syncCallBool(void* ctx, StorageBoolFn fn, bool arg, int timeoutMs) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    if (fn(ctx, arg, syncCallback, sctx) != RET_OK) {
        delete sctx;
        return {false, "Failed to send command."};
    }
    return waitSync(sctx, timeoutMs);
}

static SyncResult syncCallString(void* ctx, StorageStringFn fn,
                                  const std::string& arg, int timeoutMs) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    sctx->lifetimeArg = arg;
    if (fn(ctx, sctx->lifetimeArg.c_str(), syncCallback, sctx) != RET_OK) {
        delete sctx;
        return {false, "Failed to send command."};
    }
    return waitSync(sctx, timeoutMs);
}

static SyncResult syncCallStringAndSize(void* ctx, StorageStringIntFn fn,
                                      const std::string& arg, size_t n,
                                      int timeoutMs) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    sctx->lifetimeArg = arg;
    if (fn(ctx, sctx->lifetimeArg.c_str(), n, syncCallback, sctx) != RET_OK) {
        delete sctx;
        return {false, "Failed to send command."};
    }
    return waitSync(sctx, timeoutMs);
}

struct DownloadLeaseCleanupCtx {
    std::shared_ptr<DownloadRegistry> registry;
    std::shared_ptr<DownloadLease> lease;
};

static void downloadLeaseCleanupCallback(int ret, const char*, size_t,
                                         void* userData) {
    if (!userData || ret == RET_PROGRESS) return;
    auto* ctx = static_cast<DownloadLeaseCleanupCtx*>(userData);
    if (ret == RET_OK) {
        releaseDownloadLease(ctx->registry, ctx->lease);
    } else {
        setDownloadLeasePhase(ctx->registry, ctx->lease, DownloadPhase::Cleaning);
    }
    delete ctx;
}

static void enqueueDownloadLeaseCleanup(
    void* storageCtx, const std::shared_ptr<DownloadRegistry>& registry,
    const std::shared_ptr<DownloadLease>& lease) {
    if (!registry || !lease) return;

    reapFinishedDownloadLeaseCleanupWorkers(registry);
    bool releaseWithoutCancel = false;
    try {
        std::lock_guard<std::mutex> lock(registry->mutex);
        if (!storageCtx || registry->closing) {
            releaseWithoutCancel = true;
        } else {
            const auto current = registry->leasesByCid.find(lease->cid);
            if (current != registry->leasesByCid.end() && current->second == lease) {
                lease->phase = DownloadPhase::Cleaning;
            }
            const auto workerFinished = std::make_shared<std::atomic<bool>>(false);
            registry->cleanupWorkers.reserve(registry->cleanupWorkers.size() + 1);
            registry->cleanupWorkers.push_back(
                {workerFinished, std::thread([storageCtx, registry, lease, workerFinished] {
                    if (isDownloadRegistryClosing(registry)) {
                        releaseDownloadLease(registry, lease);
                    } else {
                        try {
                            auto* ctx = new DownloadLeaseCleanupCtx{registry, lease};
                            // The libstorage callback owns ctx for both success and
                            // immediate dispatch failure. Do not block its worker.
                            (void)storage_download_cancel(
                                storageCtx, lease->cid.c_str(),
                                downloadLeaseCleanupCallback, ctx);
                        } catch (const std::exception&) {
                            setDownloadLeasePhase(registry, lease, DownloadPhase::Cleaning);
                        }
                    }
                    workerFinished->store(true, std::memory_order_release);
                })});
        }
    } catch (const std::exception&) {
        setDownloadLeasePhase(registry, lease, DownloadPhase::Cleaning);
    }
    if (releaseWithoutCancel) releaseDownloadLease(registry, lease);
}

static void joinDownloadLeaseCleanupWorkers(
    const std::shared_ptr<DownloadRegistry>& registry) {
    std::vector<DownloadLeaseCleanupWorker> workers;
    {
        std::lock_guard<std::mutex> lock(registry->mutex);
        workers.swap(registry->cleanupWorkers);
    }
    for (DownloadLeaseCleanupWorker& worker : workers) {
        if (worker.thread.joinable()) worker.thread.join();
    }
}

static void finalizeLegacyDownloadStreamDispatch(DownloadStreamCtx* ctx,
                                                 void* storageCtx, int dispatch) {
    bool terminalReceived = false;
    {
        std::lock_guard<std::mutex> lock(ctx->dispatchMutex);
        ctx->dispatchResolved = true;
        ctx->dispatchAccepted = dispatch == RET_OK;
        terminalReceived = ctx->terminalReceived;
    }
    if (dispatch == RET_OK && terminalReceived) {
        releaseDownloadLease(ctx->registry, ctx->lease);
    } else if (dispatch != RET_OK) {
        const SyncResult cleanup = syncCallString(
            storageCtx, storage_download_cancel, ctx->cid, 1000);
        if (cleanup.ok) {
            releaseDownloadLease(ctx->registry, ctx->lease);
        } else {
            setDownloadLeasePhase(ctx->registry, ctx->lease, DownloadPhase::Cleaning);
        }
    }
    ctx->releaseReference();
}

struct LegacyDownloadInitCtx {
    std::mutex mutex;
    std::condition_variable ready;
    std::shared_ptr<DownloadRegistry> registry;
    std::shared_ptr<DownloadLease> lease;
    void* storageCtx;
    bool received = false;
    bool timedOut = false;
    int resultCode = RET_ERR;
    std::string resultMessage;
};

struct LegacyDownloadInitResult {
    bool ok = false;
    bool timedOut = false;
    std::string message;
};

static void legacyDownloadInitCallback(int ret, const char* msg, size_t len,
                                       void* userData) {
    if (!userData || ret == RET_PROGRESS) return;
    auto* ctx = static_cast<LegacyDownloadInitCtx*>(userData);
    bool timedOut = false;
    std::shared_ptr<DownloadRegistry> registry;
    std::shared_ptr<DownloadLease> lease;
    void* storageCtx = nullptr;
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);
        ctx->received = true;
        ctx->resultCode = ret;
        ctx->resultMessage = fromMsg(msg, len);
        timedOut = ctx->timedOut;
        if (timedOut) {
            registry = ctx->registry;
            lease = ctx->lease;
            storageCtx = ctx->storageCtx;
        }
        ctx->ready.notify_all();
    }
    if (!timedOut) return;

    if (ret == RET_OK) {
        setDownloadLeasePhase(registry, lease, DownloadPhase::Cleaning);
        enqueueDownloadLeaseCleanup(storageCtx, registry, lease);
    } else {
        releaseDownloadLease(registry, lease);
    }
    delete ctx;
}

static LegacyDownloadInitResult waitForLegacyDownloadInit(
    LegacyDownloadInitCtx* ctx, int timeoutMs) {
    LegacyDownloadInitResult result;
    bool deleteContext = false;
    {
        std::unique_lock<std::mutex> lock(ctx->mutex);
        ctx->ready.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [ctx] { return ctx->received; });
        if (ctx->received) {
            result.ok = ctx->resultCode == RET_OK;
            result.message = ctx->resultMessage;
            deleteContext = true;
        } else {
            ctx->timedOut = true;
            result.timedOut = true;
            result.message = "Timed out waiting for download initialization.";
            setDownloadLeasePhase(ctx->registry, ctx->lease,
                                  DownloadPhase::AwaitingLateInit);
        }
    }
    if (deleteContext) delete ctx;
    return result;
}

static LegacyDownloadInitResult startLegacyDownloadInit(
    void* storageCtx, const std::string& cid, size_t chunkSize, bool local,
    const std::shared_ptr<DownloadRegistry>& registry,
    const std::shared_ptr<DownloadLease>& lease, int timeoutMs) {
    if (!storageCtx) return {false, false, "Storage context not initialized."};
    auto* ctx = new LegacyDownloadInitCtx{
        {}, {}, registry, lease, storageCtx, false, false, RET_ERR, {}};
    if (storage_download_init(storageCtx, cid.c_str(), chunkSize, local,
                              legacyDownloadInitCallback, ctx) != RET_OK) {
        std::string error = "Failed to send download initialization.";
        {
            std::lock_guard<std::mutex> lock(ctx->mutex);
            if (!ctx->resultMessage.empty()) error = ctx->resultMessage;
        }
        delete ctx;
        return {false, false, error};
    }
    return waitForLegacyDownloadInit(ctx, timeoutMs);
}

// ---------------------------------------------------------------------------
// StorageModuleImpl
// ---------------------------------------------------------------------------

StorageModuleImpl::StorageModuleImpl()
    : storageCtx(nullptr), lifecycleState(STORAGE_LIFECYCLE_NOT_INITIALIZED),
      lifecycleGeneration(0), lifecycleInstanceId(makeNodeLifecycleInstanceId()),
      lifecycleUpdatedAtMs(nodeLifecycleTimestampMs()),
      downloadRegistry(std::make_shared<DownloadRegistry>()) {
    fprintf(stderr, "StorageModuleImpl: Initializing...\n");
}

StorageModuleImpl::~StorageModuleImpl() {
    lifecycleGeneration.fetch_add(1);
    lifecycleState.store(STORAGE_LIFECYCLE_NOT_INITIALIZED);
    cancelAndJoinDownloadV2Workers();
    if (storageCtx) {
        fprintf(stderr,
                "StorageModuleImpl: Warning - storage context was not "
                "destroyed before plugin destruction\n");
        storageCtx = nullptr;
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

std::string StorageModuleImpl::lifecycleSnapshotLocked() const {
    const std::uint8_t state = lifecycleState.load();
    json snapshot;
    snapshot["schema"] = NODE_LIFECYCLE_SNAPSHOT_SCHEMA;
    snapshot["version"] = 1;
    snapshot["instance_id"] = lifecycleInstanceId;
    snapshot["epoch"] = lifecycleEpoch;
    snapshot["sequence"] = lifecycleSequence;
    snapshot["scope"] = {{"kind", "storage"}};
    snapshot["state"] = nodeLifecycleStateName(state);
    snapshot["health"] = lifecycleError.empty() ? "unknown" : "degraded";
    snapshot["supported_actions"] = nodeLifecycleActions(state);
    snapshot["pending_operation"] = nullptr;
    if (!activeLifecycleOperationId.empty()) {
        const auto pending = lifecycleOperations.find(activeLifecycleOperationId);
        if (pending != lifecycleOperations.end() && !pending->second.settled) {
            snapshot["pending_operation"] = {
                {"operation_id", activeLifecycleOperationId},
                {"action", pending->second.action},
            };
        }
    }
    snapshot["last_completed_operation"] = nullptr;
    if (!completedLifecycleOperationIds.empty()) {
        const auto completed = lifecycleOperations.find(
            completedLifecycleOperationIds.back());
        if (completed != lifecycleOperations.end() && completed->second.settled) {
            snapshot["last_completed_operation"] = {
                {"operation_id", completedLifecycleOperationIds.back()},
                {"action", completed->second.action},
                {"outcome", completed->second.outcome},
            };
        }
    }
    snapshot["last_error"] = lifecycleError.empty()
        ? json(nullptr)
        : nodeLifecycleError(lifecycleErrorCode, lifecycleError, lifecycleErrorAtMs);
    snapshot["updated_at_ms"] = lifecycleUpdatedAtMs;
    return snapshot.dump();
}

std::string StorageModuleImpl::lifecycleEventLocked(const std::string& action,
                                                     const std::string& operationId,
                                                     const std::string& phase,
                                                     const std::string& outcome,
                                                     std::uint8_t previousState,
                                                     const std::string& errorCode,
                                                     const std::string& errorMessage) const {
    json event;
    event["schema"] = NODE_LIFECYCLE_EVENT_SCHEMA;
    event["version"] = 1;
    event["instance_id"] = lifecycleInstanceId;
    event["epoch"] = lifecycleEpoch;
    event["sequence"] = lifecycleSequence;
    event["scope"] = {{"kind", "storage"}};
    event["operation_id"] = operationId.empty() ? json(nullptr) : json(operationId);
    event["action"] = action;
    event["phase"] = phase;
    event["outcome"] = outcome;
    event["previous_state"] = nodeLifecycleStateName(previousState);
    event["status"] = json::parse(lifecycleSnapshotLocked());
    event["error"] = errorCode.empty()
        ? json(nullptr)
        : nodeLifecycleError(errorCode, errorMessage, nodeLifecycleTimestampMs());
    event["emitted_at_ms"] = nodeLifecycleTimestampMs();
    return event.dump();
}

void StorageModuleImpl::emitLifecycleEvents(const std::vector<std::string>& events) {
    for (const auto& event : events) {
        nodeChanged(event);
    }
}

void StorageModuleImpl::rememberCompletedLifecycleOperationLocked(
    const std::string& operationId) {
    if (operationId.empty()) return;
    completedLifecycleOperationIds.push_back(operationId);
    while (completedLifecycleOperationIds.size()
           > MAX_COMPLETED_NODE_LIFECYCLE_OPERATIONS) {
        const std::string expired = completedLifecycleOperationIds.front();
        completedLifecycleOperationIds.pop_front();
        const auto found = lifecycleOperations.find(expired);
        if (found != lifecycleOperations.end() && found->second.settled) {
            lifecycleOperations.erase(found);
        }
    }
}

StorageModuleImpl::LifecycleDispatch StorageModuleImpl::beginLifecycleAction(
    const std::string& action, const std::string& operationId,
    const std::string& requestFingerprint, bool hasExpectedSnapshot,
    const std::string& expectedInstanceId, std::uint64_t expectedEpoch,
    std::uint64_t expectedSequence, bool strictAction) {
    LifecycleDispatch dispatch;
    dispatch.action = action;
    dispatch.operationId = operationId;

    std::lock_guard<std::mutex> lock(lifecycleMutex);
    const auto acknowledgement = [&](bool accepted, bool duplicate,
                                     const std::string& errorCode,
                                     const std::string& errorMessage) {
        json result;
        result["schema"] = NODE_LIFECYCLE_ACK_SCHEMA;
        result["version"] = 1;
        result["operation_id"] = operationId.empty() ? json(nullptr) : json(operationId);
        result["accepted"] = accepted;
        result["duplicate"] = duplicate;
        result["instance_id"] = lifecycleInstanceId;
        result["epoch"] = lifecycleEpoch;
        result["sequence"] = lifecycleSequence;
        result["state"] = nodeLifecycleStateName(lifecycleState.load());
        result["error"] = errorCode.empty()
            ? json(nullptr)
            : nodeLifecycleError(errorCode, errorMessage, nodeLifecycleTimestampMs());
        return result.dump();
    };
    const auto settleWithoutDispatch = [&](LifecycleDispatchDisposition disposition,
                                           bool accepted,
                                           const std::string& outcome,
                                           const std::string& errorCode,
                                           const std::string& errorMessage) {
        LifecycleOperation operation;
        operation.action = action;
        operation.requestFingerprint = requestFingerprint;
        operation.accepted = accepted;
        operation.previousState = lifecycleState.load();
        const auto inserted = lifecycleOperations.emplace(operationId, std::move(operation));
        LifecycleOperation& stored = inserted.first->second;

        if (accepted) {
            ++lifecycleSequence;
            lifecycleUpdatedAtMs = nodeLifecycleTimestampMs();
            dispatch.events.push_back(lifecycleEventLocked(
                action, operationId, "accepted", "accepted", lifecycleState.load()));
        }
        stored.settled = true;
        stored.outcome = outcome;
        rememberCompletedLifecycleOperationLocked(operationId);
        ++lifecycleSequence;
        lifecycleUpdatedAtMs = nodeLifecycleTimestampMs();
        dispatch.disposition = disposition;
        dispatch.events.push_back(lifecycleEventLocked(
            action, operationId, "settled", outcome, lifecycleState.load(),
            accepted ? std::string() : errorCode,
            accepted ? std::string() : errorMessage));
        dispatch.acknowledgement = acknowledgement(
            accepted, false, accepted ? std::string() : errorCode,
            accepted ? std::string() : errorMessage);
        stored.acknowledgement = dispatch.acknowledgement;
    };

    if (strictAction) {
        const auto existing = lifecycleOperations.find(operationId);
        if (existing != lifecycleOperations.end()) {
            if (existing->second.requestFingerprint != requestFingerprint) {
                // An event with the same operation ID could be mistaken for the
                // terminal result of the original operation, so reject only in
                // the synchronous acknowledgement channel.
                dispatch.disposition = LifecycleDispatchDisposition::Rejected;
                dispatch.acknowledgement = acknowledgement(
                    false, false, "operation_id_conflict",
                    "operation_id was already used for a different request.");
                return dispatch;
            }
            dispatch.disposition = LifecycleDispatchDisposition::Duplicate;
            json duplicateAcknowledgement = json::parse(existing->second.acknowledgement);
            duplicateAcknowledgement["duplicate"] = true;
            dispatch.acknowledgement = duplicateAcknowledgement.dump();
            return dispatch;
        }
        if (!activeLifecycleOperationId.empty()) {
            const auto active = lifecycleOperations.find(activeLifecycleOperationId);
            if (active != lifecycleOperations.end() && !active->second.settled) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Rejected, false,
                                      "rejected", "operation_in_progress",
                                      "A lifecycle operation is already in progress.");
                return dispatch;
            }
        }
        if (hasExpectedSnapshot
            && (expectedInstanceId != lifecycleInstanceId
                || expectedEpoch != lifecycleEpoch
                || expectedSequence != lifecycleSequence)) {
            settleWithoutDispatch(LifecycleDispatchDisposition::Rejected, false,
                                  "rejected", "state_mismatch",
                                  "The lifecycle snapshot is stale.");
            return dispatch;
        }
    }

    const std::uint8_t state = lifecycleState.load();
    if (strictAction) {
        if (action == "initialize") {
            if (state != STORAGE_LIFECYCLE_NOT_INITIALIZED || storageCtx) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Rejected, false,
                                      "rejected", "invalid_state",
                                      "Storage is already initialized.");
                return dispatch;
            }
        } else if (action == "start") {
            if (state == STORAGE_LIFECYCLE_RUNNING) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Noop, true,
                                      "no_op", {}, {});
                return dispatch;
            }
            if (state != STORAGE_LIFECYCLE_STOPPED || !storageCtx) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Rejected, false,
                                      "rejected", "invalid_state",
                                      "Storage must be stopped before it can start.");
                return dispatch;
            }
        } else if (action == "stop") {
            if (state == STORAGE_LIFECYCLE_STOPPED) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Noop, true,
                                      "no_op", {}, {});
                return dispatch;
            }
            if (state != STORAGE_LIFECYCLE_RUNNING || !storageCtx) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Rejected, false,
                                      "rejected", "invalid_state",
                                      "Storage is not in a stoppable state.");
                return dispatch;
            }
        } else if (action == "destroy") {
            if (state == STORAGE_LIFECYCLE_NOT_INITIALIZED) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Noop, true,
                                      "no_op", {}, {});
                return dispatch;
            }
            if (state != STORAGE_LIFECYCLE_STOPPED || !storageCtx) {
                settleWithoutDispatch(LifecycleDispatchDisposition::Rejected, false,
                                      "rejected", "invalid_state",
                                      "Storage must be stopped before it can be destroyed.");
                return dispatch;
            }
        }
    }

    if (!activeLifecycleOperationId.empty()) {
        const auto active = lifecycleOperations.find(activeLifecycleOperationId);
        if (active != lifecycleOperations.end() && !active->second.settled) {
            active->second.settled = true;
            active->second.outcome = "failed";
            ++lifecycleSequence;
            lifecycleUpdatedAtMs = nodeLifecycleTimestampMs();
            dispatch.events.push_back(lifecycleEventLocked(
                active->second.action, activeLifecycleOperationId, "settled", "failed",
                active->second.previousState,
                "superseded", "Superseded by a legacy lifecycle action."));
            rememberCompletedLifecycleOperationLocked(activeLifecycleOperationId);
        }
        activeLifecycleOperationId.clear();
    }

    std::uint8_t nextState = STORAGE_LIFECYCLE_NOT_INITIALIZED;
    if (action == "initialize") nextState = STORAGE_LIFECYCLE_INITIALIZING;
    else if (action == "start") nextState = STORAGE_LIFECYCLE_STARTING;
    else if (action == "stop") nextState = STORAGE_LIFECYCLE_STOPPING;
    else if (action == "destroy") nextState = STORAGE_LIFECYCLE_DESTROYING;

    dispatch.previousState = state;
    dispatch.generation = lifecycleGeneration.fetch_add(1) + 1;
    lifecycleState.store(nextState);
    lifecycleError.clear();
    lifecycleErrorCode.clear();
    lifecycleErrorAtMs = 0;
    ++lifecycleSequence;
    lifecycleUpdatedAtMs = nodeLifecycleTimestampMs();
    dispatch.disposition = LifecycleDispatchDisposition::Dispatch;

    if (strictAction) {
        LifecycleOperation operation;
        operation.action = action;
        operation.requestFingerprint = requestFingerprint;
        operation.accepted = true;
        operation.previousState = state;
        const auto inserted = lifecycleOperations.emplace(operationId, std::move(operation));
        activeLifecycleOperationId = operationId;
        dispatch.events.push_back(lifecycleEventLocked(
            action, operationId, "accepted", "accepted", state));
        dispatch.acknowledgement = acknowledgement(true, false, {}, {});
        inserted.first->second.acknowledgement = dispatch.acknowledgement;
    } else {
        dispatch.events.push_back(lifecycleEventLocked(
            action, {}, "accepted", "accepted", state));
    }
    return dispatch;
}

void StorageModuleImpl::settleLifecycleAction(const std::string& action,
                                               const std::string& operationId,
                                               std::uint64_t generation,
                                               std::uint8_t previousState,
                                               std::uint8_t successState,
                                               std::uint8_t failureState,
                                               bool success) {
    std::string event;
    {
        std::lock_guard<std::mutex> lock(lifecycleMutex);
        if (lifecycleGeneration.load() != generation) return;

        lifecycleState.store(success ? successState : failureState);
        lifecycleError = success ? std::string() : lifecycleFailureMessage(action);
        lifecycleErrorCode = success ? std::string() : lifecycleFailureCode(action);
        lifecycleErrorAtMs = success ? 0 : nodeLifecycleTimestampMs();
        if (success && action == "initialize") ++lifecycleEpoch;

        if (!operationId.empty()) {
            const auto operation = lifecycleOperations.find(operationId);
            if (operation != lifecycleOperations.end()) {
                operation->second.settled = true;
                operation->second.outcome = success ? "succeeded" : "failed";
                rememberCompletedLifecycleOperationLocked(operationId);
            }
            if (activeLifecycleOperationId == operationId) {
                activeLifecycleOperationId.clear();
            }
        }

        ++lifecycleSequence;
        lifecycleUpdatedAtMs = nodeLifecycleTimestampMs();
        event = lifecycleEventLocked(action, operationId, "settled",
                                     success ? "succeeded" : "failed",
                                     previousState,
                                     success ? std::string() : lifecycleErrorCode,
                                     success ? std::string() : lifecycleError);
    }
    nodeChanged(event);
}

bool StorageModuleImpl::initializePrepared(const std::string& cfg,
                                            const LifecycleDispatch& dispatch) {
    auto* sctx = new SyncCtx();
    storageCtx = storage_new(cfg.c_str(), syncCallback, sctx);
    const SyncResult result = waitSync(sctx, 1000);
    if (!result.ok || !storageCtx) {
        storageCtx = nullptr;
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_STOPPED,
                              STORAGE_LIFECYCLE_NOT_INITIALIZED, false);
        return false;
    }
    settleLifecycleAction(dispatch.action, dispatch.operationId,
                          dispatch.generation, dispatch.previousState,
                          STORAGE_LIFECYCLE_STOPPED,
                          STORAGE_LIFECYCLE_NOT_INITIALIZED, true);
    return true;
}

bool StorageModuleImpl::startPrepared(const LifecycleDispatch& dispatch) {
    if (!storageCtx) {
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_RUNNING, dispatch.previousState, false);
        return false;
    }
    auto* ctx = new SimpleEventCtx(
        this, &StorageModuleImpl::storageStart, &lifecycleGeneration,
        dispatch.generation, dispatch.action, dispatch.operationId,
        dispatch.previousState, STORAGE_LIFECYCLE_RUNNING, dispatch.previousState);
    if (storage_start(storageCtx, asyncCallback, ctx) != RET_OK) {
        delete ctx;
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_RUNNING, dispatch.previousState, false);
        return false;
    }
    return true;
}

StdLogosResult StorageModuleImpl::stopPrepared(const LifecycleDispatch& dispatch) {
    if (!storageCtx) {
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_STOPPED, dispatch.previousState, false);
        return {false, {}, "Storage context not initialized."};
    }
    auto* ctx = new SimpleEventCtx(
        this, &StorageModuleImpl::storageStop, &lifecycleGeneration,
        dispatch.generation, dispatch.action, dispatch.operationId,
        dispatch.previousState, STORAGE_LIFECYCLE_STOPPED, dispatch.previousState);
    if (storage_stop(storageCtx, asyncCallback, ctx) != RET_OK) {
        delete ctx;
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_STOPPED, dispatch.previousState, false);
        return {false, {}, "Failed to send stop command."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::destroyPrepared(const LifecycleDispatch& dispatch) {
    if (!storageCtx) {
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_NOT_INITIALIZED,
                              dispatch.previousState, false);
        return {false, {}, "Storage context not initialized."};
    }
    cancelAndJoinDownloadV2Workers();
    syncCallNoArg(storageCtx, storage_close, 1000);
    const int result = storage_destroy(storageCtx);
    if (result == RET_OK) {
        storageCtx = nullptr;
        settleLifecycleAction(dispatch.action, dispatch.operationId,
                              dispatch.generation, dispatch.previousState,
                              STORAGE_LIFECYCLE_NOT_INITIALIZED,
                              dispatch.previousState, true);
        return {true, {}, ""};
    }
    settleLifecycleAction(dispatch.action, dispatch.operationId,
                          dispatch.generation, dispatch.previousState,
                          STORAGE_LIFECYCLE_NOT_INITIALIZED,
                          dispatch.previousState, false);
    return {false, {}, "Failed to destroy storage context."};
}

bool StorageModuleImpl::init(const std::string& cfg) {
    fprintf(stderr, "StorageModuleImpl::init called\n");
    if (storageCtx) {
        fprintf(stderr, "StorageModuleImpl::init: context already initialized\n");
        return false;
    }
    const LifecycleDispatch dispatch = beginLifecycleAction(
        "initialize", {}, {}, false, {}, 0, 0, false);
    emitLifecycleEvents(dispatch.events);
    return dispatch.disposition == LifecycleDispatchDisposition::Dispatch
        && initializePrepared(cfg, dispatch);
}

bool StorageModuleImpl::start() {
    fprintf(stderr, "StorageModuleImpl::start called\n");
    if (!storageCtx) {
        fprintf(stderr, "StorageModuleImpl::start: context not initialized\n");
        return false;
    }
    const LifecycleDispatch dispatch = beginLifecycleAction(
        "start", {}, {}, false, {}, 0, 0, false);
    emitLifecycleEvents(dispatch.events);
    return dispatch.disposition == LifecycleDispatchDisposition::Dispatch
        && startPrepared(dispatch);
}

StdLogosResult StorageModuleImpl::stop() {
    fprintf(stderr, "StorageModuleImpl::stop called\n");
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    const LifecycleDispatch dispatch = beginLifecycleAction(
        "stop", {}, {}, false, {}, 0, 0, false);
    emitLifecycleEvents(dispatch.events);
    if (dispatch.disposition != LifecycleDispatchDisposition::Dispatch) {
        return {false, {}, "Failed to prepare stop command."};
    }
    return stopPrepared(dispatch);
}

StdLogosResult StorageModuleImpl::destroy() {
    fprintf(stderr, "StorageModuleImpl::destroy called\n");
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    const LifecycleDispatch dispatch = beginLifecycleAction(
        "destroy", {}, {}, false, {}, 0, 0, false);
    emitLifecycleEvents(dispatch.events);
    if (dispatch.disposition != LifecycleDispatchDisposition::Dispatch) {
        return {false, {}, "Failed to prepare destruction."};
    }
    return destroyPrepared(dispatch);
}

// ---------------------------------------------------------------------------
// Info
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::version() {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    char* v = storage_version(storageCtx);
    if (!v) return {false, {}, "Failed to get version."};
    std::string result(v);
    free(v);
    return {true, result, ""};
}

std::string StorageModuleImpl::moduleVersion() {
    return STORAGE_MODULE_VERSION;
}

LogosMap StorageModuleImpl::lifecycleStatus() {
    const std::uint8_t state = lifecycleState.load();
    return {
        {"initialized", state != STORAGE_LIFECYCLE_NOT_INITIALIZED
                            && state != STORAGE_LIFECYCLE_INITIALIZING},
        {"running", state == STORAGE_LIFECYCLE_RUNNING},
        {"state", storageLifecycleStateName(state)},
    };
}

std::string StorageModuleImpl::nodeStatus() {
    std::lock_guard<std::mutex> lock(lifecycleMutex);
    return lifecycleSnapshotLocked();
}

std::string StorageModuleImpl::nodeAction(const std::string& request) {
    const auto rejected = [this](const std::string& code,
                                 const std::string& message) {
        std::lock_guard<std::mutex> lock(lifecycleMutex);
        json result;
        result["schema"] = NODE_LIFECYCLE_ACK_SCHEMA;
        result["version"] = 1;
        result["operation_id"] = nullptr;
        result["accepted"] = false;
        result["duplicate"] = false;
        result["instance_id"] = lifecycleInstanceId;
        result["epoch"] = lifecycleEpoch;
        result["sequence"] = lifecycleSequence;
        result["state"] = nodeLifecycleStateName(lifecycleState.load());
        result["error"] = nodeLifecycleError(
            code, message, nodeLifecycleTimestampMs());
        return result.dump();
    };

    if (request.size() > MAX_NODE_LIFECYCLE_REQUEST_BYTES) {
        return rejected("request_too_large",
                        "Lifecycle request exceeds the supported size.");
    }

    json input;
    try {
        input = json::parse(request);
    } catch (const std::exception&) {
        return rejected("invalid_request", "Lifecycle request must be a JSON object.");
    }
    if (!input.is_object()) {
        return rejected("invalid_request", "Lifecycle request must be a JSON object.");
    }
    for (const auto& item : input.items()) {
        const std::string& key = item.key();
        if (key != "schema" && key != "version" && key != "operation_id"
            && key != "action" && key != "expected" && key != "parameters") {
            return rejected("invalid_request", "Lifecycle request contains an unsupported field.");
        }
    }

    const auto schema = input.find("schema");
    if (schema == input.end() || !schema->is_string()
        || schema->get<std::string>() != NODE_LIFECYCLE_COMMAND_SCHEMA) {
        return rejected("invalid_request", "Unsupported lifecycle request schema.");
    }
    const auto version = input.find("version");
    if (version == input.end() || !version->is_number_integer()
        || version->get<int>() != 1) {
        return rejected("invalid_request", "Unsupported lifecycle request version.");
    }
    const auto operationId = input.find("operation_id");
    if (operationId == input.end() || !operationId->is_string()) {
        return rejected("invalid_request", "Lifecycle request requires an operation_id.");
    }
    const std::string operation = operationId->get<std::string>();
    if (!isValidNodeLifecycleOperationId(operation)) {
        return rejected("invalid_request", "Lifecycle operation_id is invalid.");
    }
    const auto actionValue = input.find("action");
    if (actionValue == input.end() || !actionValue->is_string()) {
        return rejected("invalid_request", "Lifecycle request requires an action.");
    }
    const std::string action = actionValue->get<std::string>();
    if (action != "initialize" && action != "start"
        && action != "stop" && action != "destroy") {
        return rejected("invalid_request", "Unsupported lifecycle action.");
    }

    const auto parseUnsigned = [](const json& value, std::uint64_t& parsed) {
        if (value.is_number_unsigned()) {
            parsed = value.get<std::uint64_t>();
            return true;
        }
        if (value.is_number_integer()) {
            const auto signedValue = value.get<std::int64_t>();
            if (signedValue >= 0) {
                parsed = static_cast<std::uint64_t>(signedValue);
                return true;
            }
        }
        return false;
    };

    bool hasExpectedSnapshot = false;
    std::string expectedInstanceId;
    std::uint64_t expectedEpoch = 0;
    std::uint64_t expectedSequence = 0;
    const auto expected = input.find("expected");
    if (expected != input.end()) {
        if (!expected->is_object() || expected->size() != 3
            || !expected->contains("instance_id")
            || !expected->contains("epoch")
            || !expected->contains("sequence")
            || !expected->at("instance_id").is_string()
            || !parseUnsigned(expected->at("epoch"), expectedEpoch)
            || !parseUnsigned(expected->at("sequence"), expectedSequence)) {
            return rejected("invalid_request",
                            "Lifecycle expected snapshot must contain instance_id, epoch, and sequence.");
        }
        expectedInstanceId = expected->at("instance_id").get<std::string>();
        hasExpectedSnapshot = true;
    }

    json parameters = json::object();
    const auto parametersValue = input.find("parameters");
    if (parametersValue != input.end()) {
        if (!parametersValue->is_object()) {
            return rejected("invalid_request", "Lifecycle parameters must be an object.");
        }
        parameters = *parametersValue;
    }

    std::string initializationConfig;
    if (action == "initialize") {
        const auto config = parameters.find("config");
        if (config == parameters.end() || !config->is_string()) {
            return rejected("invalid_request", "Initialize requires parameters.config.");
        }
        if (parameters.size() != 1) {
            return rejected("invalid_request", "Initialize accepts only parameters.config.");
        }
        initializationConfig = config->get<std::string>();
        if (initializationConfig.size() > MAX_NODE_LIFECYCLE_CONFIG_BYTES
            || containsEmbeddedNul(initializationConfig)) {
            return rejected("invalid_request",
                            "Initialize config is invalid or exceeds the supported size.");
        }
    } else if (!parameters.empty()) {
        return rejected("invalid_request",
                        "This lifecycle action does not accept parameters.");
    }

    const std::string requestFingerprint = input.dump();
    const LifecycleDispatch dispatch = beginLifecycleAction(
        action, operation, requestFingerprint, hasExpectedSnapshot,
        expectedInstanceId, expectedEpoch, expectedSequence, true);
    emitLifecycleEvents(dispatch.events);
    if (dispatch.disposition != LifecycleDispatchDisposition::Dispatch) {
        return dispatch.acknowledgement;
    }

    if (action == "initialize") {
        initializePrepared(initializationConfig, dispatch);
    } else if (action == "start") {
        startPrepared(dispatch);
    } else if (action == "stop") {
        stopPrepared(dispatch);
    } else {
        destroyPrepared(dispatch);
    }
    return dispatch.acknowledgement;
}

StdLogosResult StorageModuleImpl::dataDir() {
    auto r = syncCallNoArg(storageCtx, storage_repo, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::peerId() {
    auto r = syncCallNoArg(storageCtx, storage_peer_id, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::spr() {
    auto r = syncCallNoArg(storageCtx, storage_spr, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::debug() {
    auto r = syncCallNoArg(storageCtx, storage_debug, 1000);
    if (!r.ok) return {false, {}, r.message};
    try {
        auto parsed = json::parse(r.message);
        // libstorage v0.4.2 renamed the provider-record field to
        // `providerAddresses`. Keep the module's established
        // `announceAddresses` contract for existing Inspector consumers.
        if (parsed.is_object() && !parsed.contains("announceAddresses")) {
            const auto providerAddresses = parsed.find("providerAddresses");
            if (providerAddresses != parsed.end() && providerAddresses->is_array()) {
                parsed["announceAddresses"] = *providerAddresses;
            }
        }
        return {true, std::move(parsed), ""};
    } catch (...) {
        return {false, {}, "Failed to parse debug info."};
    }
}

LogosMap StorageModuleImpl::collectMetrics() {
    auto emptyMetrics = [] { return json{{"metrics", json::array()}}; };

    auto r = syncCallNoArg(storageCtx, storage_get_metrics, 1000);
    if (!r.ok) return emptyMetrics();
    try {
        json parsed = json::parse(r.message);
        if (!parsed.is_object()) return emptyMetrics();
        auto metrics = parsed.find("metrics");
        if (metrics == parsed.end() || !metrics->is_array()) return emptyMetrics();
        return parsed;
    } catch (...) {
        return emptyMetrics();
    }
}

StdLogosResult StorageModuleImpl::updateLogLevel(const std::string& logLevel) {
    auto r = syncCallString(storageCtx, storage_log_level, logLevel, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::connect(const std::string& peerId,
                                           const std::vector<std::string>& peerAddresses) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    std::vector<char*> addrs;
    addrs.reserve(peerAddresses.size());
    for (const auto& a : peerAddresses) addrs.push_back(strdup(a.c_str()));

    auto* ctx = new ConnectCtx(this, peerId, addrs);
    if (storage_connect(storageCtx, ctx->peerIdBuf.c_str(),
                        const_cast<const char**>(ctx->addrs.data()),
                        ctx->addrs.size(), asyncCallback, ctx) != RET_OK) {
        return {false, {}, "Failed to send connect command."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::togglePrivateQueries(bool enabled) {
    auto r = syncCallBool(storageCtx, storage_toggle_private_queries, enabled, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message == "true", ""};
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::uploadInit(const std::string& filename,
                                              int64_t chunkSize) {
    auto r = syncCallStringAndSize(storageCtx, storage_upload_init, filename,
                                static_cast<size_t>(chunkSize), 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::uploadUrl(const std::string& filePath,
                                             int64_t chunkSize) {
    fprintf(stderr, "StorageModuleImpl::uploadUrl called with path=%s\n",
            filePath.c_str());
    if (!storageCtx || chunkSize <= 0)
        return {false, {}, "Invalid arguments."};

    std::error_code ec;
    if (!fs::exists(filePath, ec) || !fs::is_regular_file(filePath, ec)) {
        fprintf(stderr, "StorageModuleImpl::uploadUrl: file not found or not regular: %s\n",
                filePath.c_str());
        return {false, {}, "File not found: " + filePath};
    }

    int64_t fileSize = static_cast<int64_t>(fs::file_size(filePath, ec));

    auto ir = syncCallStringAndSize(storageCtx, storage_upload_init, filePath,
                                  static_cast<size_t>(chunkSize), 1000);
    if (!ir.ok)
        return {false, {}, ir.message};
    std::string sessionId = ir.message;

    auto* ctx = new UploadFileCtx(this, sessionId, fileSize);
    if (storage_upload_file(storageCtx, ctx->sessionId.c_str(),
                            asyncCallback, ctx) != RET_OK) {
        syncCallString(storageCtx, storage_upload_cancel, sessionId, 1000);
        return {false, {}, "Failed to start file upload."};
    }
    return {true, sessionId, ""};
}

StdLogosResult StorageModuleImpl::uploadChunk(const std::string& sessionId,
                                               const std::string& chunk) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new UploadChunkCtx(this, sessionId, chunk);
    const auto* data = reinterpret_cast<const uint8_t*>(ctx->chunk.data());
    if (storage_upload_chunk(storageCtx, ctx->sessionId.c_str(), data,
                             ctx->chunk.size(), asyncCallback, ctx) != RET_OK) {
        return {false, {}, "Failed to send chunk."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::uploadFinalize(const std::string& sessionId) {
    auto r = syncCallString(storageCtx, storage_upload_finalize, sessionId, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message, ""};
}

StdLogosResult StorageModuleImpl::uploadCancel(const std::string& sessionId) {
    auto r = syncCallString(storageCtx, storage_upload_cancel, sessionId, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

std::string StorageModuleImpl::downloadChunksInternal(const std::string& cid,
                                                       const std::string& filepath,
                                                       bool local,
                                                       int64_t chunkSize) {
    reapFinishedDownloadLeaseCleanupWorkers(downloadRegistry);
    if (!storageCtx || chunkSize <= 0 || chunkSize > MAX_DOWNLOAD_V2_CHUNK_BYTES
        || cid.empty() || containsEmbeddedNul(cid)
        || containsEmbeddedNul(filepath)) {
        return {};
    }

    std::string reserveError;
    const auto lease = reserveDownloadLease(downloadRegistry, cid, DownloadOwner::Legacy,
                                            {}, reserveError);
    if (!lease) return {};

    // For file-mode download, fetch the manifest to determine total size so
    // that progress events can be throttled to one per percentage point.
    int64_t totalBytes = 0;
    if (!filepath.empty()) {
        auto mr = syncCallString(storageCtx, storage_download_manifest, cid, 3000);
        if (mr.ok) {
            try {
                json manifest = json::parse(mr.message);
                if (manifest.contains("datasetSize") && !manifest["datasetSize"].is_null()) {
                    const auto& ds = manifest["datasetSize"];
                    if (ds.is_number_integer()) totalBytes = ds.get<int64_t>();
                    else if (ds.is_number()) totalBytes = static_cast<int64_t>(ds.get<double>());
                    else if (ds.is_string()) totalBytes = std::stoll(ds.get_ref<const std::string&>());
                }
            } catch (const std::exception& e) {
                fprintf(stderr,
                        "StorageModuleImpl::downloadChunksInternal: failed to parse manifest for %s: %s\n",
                        cid.c_str(), e.what());
            } catch (...) {
                fprintf(stderr,
                        "StorageModuleImpl::downloadChunksInternal: failed to parse manifest for %s (unknown error)\n",
                        cid.c_str());
            }
        }
        if (totalBytes == 0) {
            fprintf(stderr,
                    "StorageModuleImpl::downloadChunksInternal: failed to get "
                    "manifest for %s\n",
                    cid.c_str());
            releaseDownloadLease(downloadRegistry, lease);
            return {};
        }
    }

    const auto initialized = startLegacyDownloadInit(
        storageCtx, cid, static_cast<size_t>(chunkSize), local, downloadRegistry, lease, 1000);
    if (!initialized.ok) {
        if (!initialized.timedOut) releaseDownloadLease(downloadRegistry, lease);
        return {};
    }
    setDownloadLeasePhase(downloadRegistry, lease, DownloadPhase::Active);

    auto* ctx = new DownloadStreamCtx(this, cid, filepath, downloadRegistry, lease, totalBytes);
    const int dispatch = storage_download_stream(
        storageCtx, ctx->cid.c_str(), static_cast<size_t>(chunkSize), local,
        ctx->filepath.c_str(), legacyDownloadStreamCallback, ctx);
    finalizeLegacyDownloadStreamDispatch(ctx, storageCtx, dispatch);
    if (dispatch != RET_OK) {
        return {};
    }
    return cid;
}

StdLogosResult StorageModuleImpl::downloadToUrl(const std::string& cid,
                                                 const std::string& filePath,
                                                 bool local, int64_t chunkSize) {
    std::string sessionId = downloadChunksInternal(cid, filePath, local, chunkSize);
    if (sessionId.empty())
        return {false, {}, "Failed to start download."};
    return {true, sessionId, ""};
}

StdLogosResult StorageModuleImpl::downloadChunks(const std::string& cid, bool local,
                                                  int64_t chunkSize) {
    std::string sessionId = downloadChunksInternal(cid, "", local, chunkSize);
    if (sessionId.empty())
        return {false, {}, "Failed to start chunk download."};
    return {true, sessionId, ""};
}

StdLogosResult StorageModuleImpl::downloadCancel(const std::string& sessionId) {
    reapFinishedDownloadLeaseCleanupWorkers(downloadRegistry);
    if (!storageCtx) return {false, {}, "Storage context not initialized."};
    if (sessionId.empty() || containsEmbeddedNul(sessionId)) {
        return {false, {}, "Invalid download session ID."};
    }
    {
        std::lock_guard<std::mutex> lock(downloadRegistry->mutex);
        const auto lease = downloadRegistry->leasesByCid.find(sessionId);
        if (lease != downloadRegistry->leasesByCid.end()
            && lease->second->owner == DownloadOwner::Versioned) {
            return {false, {},
                    "A versioned download is active; cancel it by module operation ID."};
        }
    }
    auto r = syncCallString(storageCtx, storage_download_cancel, sessionId, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

LogosMap StorageModuleImpl::downloadProtocol() {
    return json{
        {"protocol", "logos.storage.download"},
        {"version", DOWNLOAD_PROTOCOL_VERSION},
        {"moduleOperationIdOwner", "caller"},
        {"cancelTimeoutMs", DOWNLOAD_CANCEL_TIMEOUT_MS},
        {"maxDownloadBytes", MAX_DOWNLOAD_V2_BYTES},
        {"maxChunkBytes", MAX_DOWNLOAD_V2_CHUNK_BYTES},
    };
}

StdLogosResult StorageModuleImpl::downloadToUrlV2(
    const std::string& cid, const std::string& filePath, bool local,
    int64_t chunkSize, const std::string& operationId, int64_t maxDownloadBytes) {
    reapFinishedDownloadV2Workers();
    if (!storageCtx) {
        return {false, {}, "Storage context not initialized."};
    }
    if (cid.empty() || filePath.empty() || !isValidDownloadV2OperationId(operationId)
        || containsEmbeddedNul(cid) || containsEmbeddedNul(filePath)
        || operationId == cid
        || chunkSize <= 0 || maxDownloadBytes <= 0
        || maxDownloadBytes > MAX_DOWNLOAD_V2_BYTES
        || chunkSize > MAX_DOWNLOAD_V2_CHUNK_BYTES) {
        return {false, {}, "Invalid versioned download arguments."};
    }
    const int64_t effectiveChunkSize = std::min(chunkSize, maxDownloadBytes);

    std::string reserveError;
    const auto lease = reserveDownloadLease(downloadRegistry, cid, DownloadOwner::Versioned,
                                            operationId, reserveError);
    if (!lease) return {false, {}, reserveError};

    const auto manifest = syncCallString(storageCtx, storage_download_manifest, cid, 3000);
    uint64_t manifestBytes = 0;
    if (!manifest.ok || !manifestDatasetSize(manifest.message, manifestBytes)) {
        releaseDownloadLease(downloadRegistry, lease);
        return {false, {}, "Failed to read download manifest."};
    }
    if (manifestBytes > static_cast<uint64_t>(maxDownloadBytes)) {
        releaseDownloadLease(downloadRegistry, lease);
        return {false, {}, "Download exceeds requested byte limit."};
    }

    const fs::path stagingPath = downloadV2StagingPath(filePath);
    {
        std::ofstream probe(stagingPath, std::ios::binary | std::ios::trunc);
        if (!probe) {
            releaseDownloadLease(downloadRegistry, lease);
            return {false, {}, "Failed to open download staging file."};
        }
    }

    auto state = std::make_shared<DownloadV2State>();
    state->cid = cid;
    state->operationId = operationId;
    state->registry = downloadRegistry;
    state->lease = lease;
    if (!activateDownloadV2(state)) {
        std::error_code ec;
        fs::remove(stagingPath, ec);
        releaseDownloadLease(downloadRegistry, lease);
        return {false, {}, "Storage context is shutting down."};
    }

    try {
        {
            std::lock_guard<std::mutex> lock(downloadV2WorkersMutex);
            downloadV2Workers.reserve(downloadV2Workers.size() + 1);
        }
        std::thread worker(&StorageModuleImpl::runDownloadV2, this, state,
                           stagingPath.string(), filePath, effectiveChunkSize, manifestBytes,
                           static_cast<uint64_t>(maxDownloadBytes));
        {
            std::lock_guard<std::mutex> lock(downloadV2WorkersMutex);
            downloadV2Workers.push_back({state, std::move(worker)});
        }
    } catch (const std::exception&) {
        abandonDownloadV2Start(state);
        std::error_code ec;
        fs::remove(stagingPath, ec);
        return {false, {}, "Failed to start versioned download worker."};
    }

    DownloadV2InitCtx* initCtx = nullptr;
    try {
        initCtx = new DownloadV2InitCtx{state, storageCtx};
    } catch (const std::exception&) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->dispatchResolved = true;
            state->dispatchAccepted = false;
            state->initializationResolved = true;
            state->initializationSucceeded = false;
            state->initializationError = "Failed to allocate download initialization state.";
        }
        state->initializationReady.notify_all();
        abandonDownloadV2Start(state);
        std::error_code ec;
        fs::remove(stagingPath, ec);
        return {false, {}, "Failed to start versioned download."};
    }
    const int dispatch = storage_download_init(
        storageCtx, cid.c_str(), static_cast<size_t>(effectiveChunkSize), local,
        downloadV2InitCallback, initCtx);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->dispatchResolved = true;
        state->dispatchAccepted = dispatch == RET_OK;
        if (dispatch != RET_OK && !state->initializationResolved) {
            state->initializationResolved = true;
            state->initializationSucceeded = false;
            state->initializationError = "Failed to send download initialization.";
        }
    }
    state->initializationReady.notify_all();
    if (dispatch != RET_OK) {
        // libstorage reports dispatch errors through downloadV2InitCallback,
        // which owns initCtx. Release routing state before returning so a
        // caller can immediately retry this CID or operation ID.
        abandonDownloadV2Start(state);
        std::error_code ec;
        fs::remove(stagingPath, ec);
        return {false, {}, "Failed to send download initialization."};
    }

    return {true,
            json{
                {"protocol", "logos.storage.download"},
                {"version", DOWNLOAD_PROTOCOL_VERSION},
                {"accepted", true},
                {"moduleOperationId", operationId},
                {"cid", cid},
            },
            ""};
}

StdLogosResult StorageModuleImpl::downloadCancelV2(const std::string& operationId) {
    reapFinishedDownloadV2Workers();
    if (operationId.empty()) {
        return {false, {}, "Download operation ID is required."};
    }
    if (!isValidDownloadV2OperationId(operationId)) {
        return {false, {}, "Invalid download operation ID."};
    }

    std::string cid;
    std::shared_ptr<DownloadV2State> state;
    {
        std::lock_guard<std::mutex> lock(downloadRegistry->mutex);
        const auto active = downloadRegistry->activeDownloadsV2.find(operationId);
        if (active == downloadRegistry->activeDownloadsV2.end()) {
            const auto terminal = downloadRegistry->terminalDownloadsV2.find(operationId);
            if (terminal == downloadRegistry->terminalDownloadsV2.end()) {
                return {true,
                        json{
                            {"protocol", "logos.storage.download"},
                            {"version", DOWNLOAD_PROTOCOL_VERSION},
                            {"moduleOperationId", operationId},
                            {"cancelStatus", "not_found"},
                        },
                        ""};
            }
            return {true,
                    json{
                        {"protocol", "logos.storage.download"},
                        {"version", DOWNLOAD_PROTOCOL_VERSION},
                        {"moduleOperationId", operationId},
                        {"cid", terminal->second.cid},
                        {"cancelStatus", "already_terminal"},
                        {"terminalOutcome", terminal->second.outcome},
                    },
                        ""};
        }
        cid = active->second->cid;
        state = active->second;
    }

    bool initializationPending = false;
    bool initializationFailed = false;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->cancellationRequested = true;
        initializationPending = !state->initializationResolved;
        state->cancellationBeforeInitialization = initializationPending;
        initializationFailed = state->initializationResolved
            && !state->initializationSucceeded;
    }
    if (initializationPending) {
        // An initialization callback may arrive much later. Report the caller's
        // cancellation now, retain the CID lease, and clean up the late native
        // session from a separate worker when that callback arrives.
        setDownloadLeasePhase(downloadRegistry, state->lease,
                              DownloadPhase::AwaitingLateInit);
        state->initializationReady.notify_all();
        finishDownloadV2(state, "canceled", {}, false);
        return {true,
                json{
                    {"protocol", "logos.storage.download"},
                    {"version", DOWNLOAD_PROTOCOL_VERSION},
                    {"moduleOperationId", operationId},
                    {"cid", cid},
                    {"cancelStatus", "canceled"},
                },
                ""};
    }
    if (!initializationPending && !initializationFailed
        && !requestDownloadV2Cancellation(storageCtx, state)) {
        std::lock_guard<std::mutex> lock(state->mutex);
        return {false, {}, state->cancellationError.empty()
                ? "Failed to send download cancellation."
                : state->cancellationError};
    }

    return {true,
            json{
                {"protocol", "logos.storage.download"},
                {"version", DOWNLOAD_PROTOCOL_VERSION},
                {"moduleOperationId", operationId},
                {"cid", cid},
                {"cancelStatus", "canceled"},
            },
            ""};
}

void StorageModuleImpl::runDownloadV2(
    const std::shared_ptr<DownloadV2State>& state, const std::string& stagingPath,
    const std::string& destinationPath, int64_t chunkSize, uint64_t expectedBytes,
    uint64_t maxBytes) {
    auto removePartialFile = [&stagingPath] {
        std::error_code ec;
        fs::remove(stagingPath, ec);
    };
    bool shutdownRequested = false;
    bool dispatchAccepted = false;
    bool initializationSucceeded = false;
    bool cancellationBeforeInitialization = false;
    bool cancellationWasRequested = false;
    std::string initializationError;
    {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->initializationReady.wait(lock, [&state] {
            return state->shutdownRequested
                || state->cancellationBeforeInitialization
                || (state->dispatchResolved && state->initializationResolved);
        });
        shutdownRequested = state->shutdownRequested;
        dispatchAccepted = state->dispatchAccepted;
        initializationSucceeded = state->initializationSucceeded;
        cancellationBeforeInitialization = state->cancellationBeforeInitialization;
        cancellationWasRequested = state->cancellationRequested;
        initializationError = state->initializationError;
    }
    if (shutdownRequested || !dispatchAccepted) {
        removePartialFile();
        abandonDownloadV2Start(state);
        return;
    }
    if (cancellationBeforeInitialization) {
        removePartialFile();
        state->workerFinished.store(true);
        return;
    }
    if (!initializationSucceeded) {
        removePartialFile();
        if (cancellationWasRequested) {
            finishDownloadV2(state, "canceled");
        } else {
            finishDownloadV2(
                state, "failed",
                initializationError.empty()
                    ? "Failed to initialize versioned download."
                    : initializationError);
        }
        return;
    }
    setDownloadLeasePhase(state->registry, state->lease, DownloadPhase::Active);

    std::ofstream output(stagingPath, std::ios::binary | std::ios::trunc);
    auto cancellationRequested = [&state] {
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->cancellationRequested;
    };
    auto finishCancellation = [&] {
        requestDownloadV2Cancellation(storageCtx, state);
        const SyncResult canceled = waitForDownloadV2Cancellation(state);
        output.close();
        removePartialFile();
        if (canceled.ok) {
            finishDownloadV2(state, "canceled");
        } else {
            setDownloadLeasePhase(state->registry, state->lease,
                                  DownloadPhase::Cleaning);
            finishDownloadV2(state, "failed", canceled.message, false);
        }
    };
    auto fail = [&](const std::string& error, bool cancelSession) {
        SyncResult cancellation{true, {}};
        if (cancelSession) {
            requestDownloadV2Cancellation(storageCtx, state);
            cancellation = waitForDownloadV2Cancellation(state);
        }
        output.close();
        removePartialFile();
        if (cancellationRequested() && cancellation.ok) {
            finishDownloadV2(state, "canceled");
            return;
        }
        std::string failure = error.empty() ? "Storage download failed." : error;
        if (!cancellation.ok) {
            failure += " Cleanup failed: " + cancellation.message;
        }
        if (!cancellation.ok && cancelSession) {
            setDownloadLeasePhase(state->registry, state->lease,
                                  DownloadPhase::Cleaning);
            finishDownloadV2(state, "failed", failure, false);
        } else {
            finishDownloadV2(state, "failed", failure);
        }
    };

    if (!output) {
        fail("Failed to open download staging file.", true);
        return;
    }

    uint64_t bytesWritten = 0;
    while (true) {
        if (cancellationRequested()) {
            finishCancellation();
            return;
        }

        const uint64_t remaining = bytesWritten <= maxBytes
            ? maxBytes - bytesWritten
            : 0;
        const size_t maxChunkBytes = static_cast<size_t>(
            std::min<uint64_t>(static_cast<uint64_t>(chunkSize), remaining));
        const DownloadV2ChunkResult chunk = requestDownloadV2Chunk(
            storageCtx, state->cid, maxChunkBytes);

        if (cancellationRequested()) {
            finishCancellation();
            return;
        }
        if (!chunk.dispatched || !chunk.completed) {
            fail(chunk.error.empty() ? "Failed to download chunk." : chunk.error, true);
            return;
        }
        if (!chunk.succeeded) {
            fail(chunk.error.empty() ? "Storage download chunk failed." : chunk.error, true);
            return;
        }
        if (chunk.exceededLimit
            || static_cast<uint64_t>(chunk.bytes.size()) > maxBytes - bytesWritten) {
            fail("Download exceeded requested byte limit.", true);
            return;
        }
        if (chunk.bytes.empty()) {
            if (bytesWritten != expectedBytes) {
                fail("Download size did not match its manifest.", true);
                return;
            }
            output.flush();
            if (!output) {
                fail("Failed to write download destination.", true);
                return;
            }
            output.close();
            requestDownloadV2Cancellation(storageCtx, state);
            const SyncResult cleanup = waitForDownloadV2Cancellation(state);
            if (!cleanup.ok) {
                removePartialFile();
                setDownloadLeasePhase(state->registry, state->lease,
                                      DownloadPhase::Cleaning);
                finishDownloadV2(state, "failed", cleanup.message, false);
                return;
            }
            if (cancellationRequested()) {
                removePartialFile();
                finishDownloadV2(state, "canceled");
                return;
            }
            std::string replacementError;
            if (!replaceDownloadV2Destination(stagingPath, destinationPath,
                                              replacementError)) {
                removePartialFile();
                finishDownloadV2(state, "failed", replacementError);
                return;
            }
            finishDownloadV2(state, "succeeded");
            return;
        }

        output.write(reinterpret_cast<const char*>(chunk.bytes.data()),
                     static_cast<std::streamsize>(chunk.bytes.size()));
        if (!output) {
            fail("Failed to write download destination.", true);
            return;
        }
        bytesWritten += static_cast<uint64_t>(chunk.bytes.size());
    }
}

void StorageModuleImpl::finishDownloadV2(
    const std::shared_ptr<DownloadV2State>& state, const std::string& outcome,
    const std::string& error, bool releaseLease) {
    bool releaseLeaseNow = releaseLease;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (!releaseLeaseNow && state->cancellationDispatched
            && state->cancellationResolved && state->cancellationSucceeded) {
            releaseLeaseNow = true;
        }
        state->releaseLeaseOnCancellationConfirmation =
            !releaseLeaseNow && state->cancellationDispatched;
    }
    {
        std::lock_guard<std::mutex> lock(state->registry->mutex);
        const auto active = state->registry->activeDownloadsV2.find(state->operationId);
        if (active != state->registry->activeDownloadsV2.end() && active->second == state) {
            state->registry->activeDownloadsV2.erase(active);
        }
        if (releaseLeaseNow) {
            const auto lease = state->registry->leasesByCid.find(state->cid);
            if (lease != state->registry->leasesByCid.end()
                && lease->second == state->lease) {
                state->registry->leasesByCid.erase(lease);
            }
        }
        state->registry->pendingOperationIdsV2.erase(state->operationId);
        state->registry->terminalDownloadsV2[state->operationId] = {state->cid, outcome};
        state->registry->terminalDownloadOrderV2.push_back(state->operationId);
        while (state->registry->terminalDownloadOrderV2.size()
               > MAX_TERMINAL_DOWNLOADS_V2) {
            const std::string expired = state->registry->terminalDownloadOrderV2.front();
            state->registry->terminalDownloadOrderV2.pop_front();
            state->registry->terminalDownloadsV2.erase(expired);
        }
    }

    json payload = {
        {"protocol", "logos.storage.download"},
        {"version", DOWNLOAD_PROTOCOL_VERSION},
        {"moduleOperationId", state->operationId},
        {"cid", state->cid},
        {"outcome", outcome},
    };
    if (outcome == "failed") {
        payload["error"] = error.empty() ? "Storage download failed." : error;
    }
    emitJsonEvent(this, &StorageModuleImpl::storageDownloadDoneV2, payload,
                  "StorageModuleImpl::finishDownloadV2");
    state->workerFinished.store(true);
}

void StorageModuleImpl::reapFinishedDownloadV2Workers() {
    reapFinishedDownloadLeaseCleanupWorkers(downloadRegistry);
    std::vector<std::thread> finished;
    {
        std::lock_guard<std::mutex> lock(downloadV2WorkersMutex);
        auto worker = downloadV2Workers.begin();
        while (worker != downloadV2Workers.end()) {
            if (worker->state->workerFinished.load()) {
                finished.push_back(std::move(worker->thread));
                worker = downloadV2Workers.erase(worker);
            } else {
                ++worker;
            }
        }
    }
    for (std::thread& worker : finished) {
        if (worker.joinable()) worker.join();
    }
}

void StorageModuleImpl::cancelAndJoinDownloadV2Workers() {
    closeDownloadRegistry(downloadRegistry);
    std::vector<std::shared_ptr<DownloadV2State>> active;
    {
        std::lock_guard<std::mutex> lock(downloadRegistry->mutex);
        for (const auto& entry : downloadRegistry->activeDownloadsV2) {
            active.push_back(entry.second);
        }
    }
    for (const auto& state : active) {
        bool canCancel = false;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->cancellationRequested = true;
            state->shutdownRequested = true;
            canCancel = state->initializationResolved && state->initializationSucceeded;
        }
        state->initializationReady.notify_all();
        if (canCancel) requestDownloadV2Cancellation(storageCtx, state);
    }

    std::vector<DownloadV2Worker> workers;
    {
        std::lock_guard<std::mutex> lock(downloadV2WorkersMutex);
        workers.swap(downloadV2Workers);
    }
    for (DownloadV2Worker& worker : workers) {
        if (worker.thread.joinable()) worker.thread.join();
    }
    joinDownloadLeaseCleanupWorkers(downloadRegistry);
}

// ---------------------------------------------------------------------------
// Data management
// ---------------------------------------------------------------------------

StdLogosResult StorageModuleImpl::exists(const std::string& cid) {
    auto r = syncCallString(storageCtx, storage_exists, cid, 1000);
    if (!r.ok) return {false, {}, r.message};
    return {true, r.message == "true", ""};
}

StdLogosResult StorageModuleImpl::fetch(const std::string& cid) {
    auto r = syncCallString(storageCtx, storage_fetch, cid, 3000);
    if (!r.ok) return {false, {}, r.message};
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::remove(const std::string& cid) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new RemoveCtx(this, cid);
    if (storage_delete(storageCtx, ctx->cid.c_str(), asyncCallback, ctx) !=
        RET_OK) {
        return {false, {}, "Failed to send remove command."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::space() {
    auto r = syncCallNoArg(storageCtx, storage_space, 1000);
    if (!r.ok) return {false, {}, r.message};
    try {
        return {true, json::parse(r.message), ""};
    } catch (...) {
        return {false, {}, "Failed to parse space info."};
    }
}

StdLogosResult StorageModuleImpl::manifests() {
    auto r = syncCallNoArg(storageCtx, storage_list, 1000);
    if (!r.ok) return {false, {}, r.message};
    try {
        json raw = json::parse(r.message);
        if (!raw.is_array())
            return {false, {}, "Failed to parse manifests."};
        json list = json::array();
        for (const auto& item : raw) {
            json entry;
            if (item.contains("cid"))      entry["cid"]         = item["cid"];
            if (item.contains("manifest")) {
                const auto& m = item["manifest"];
                if (m.contains("treeCid"))     entry["treeCid"]     = m["treeCid"];
                if (m.contains("datasetSize")) entry["datasetSize"] = m["datasetSize"];
                if (m.contains("blockSize"))   entry["blockSize"]   = m["blockSize"];
                if (m.contains("filename"))    entry["filename"]    = m["filename"];
                if (m.contains("mimetype"))    entry["mimetype"]    = m["mimetype"];
            }
            list.push_back(entry);
        }
        return {true, list, ""};
    } catch (...) {
        return {false, {}, "Failed to parse manifests."};
    }
}

StdLogosResult StorageModuleImpl::downloadManifest(const std::string& cid) {
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new FetchManifestCtx(this, cid);
    if (storage_download_manifest(storageCtx, ctx->cid.c_str(), asyncCallback,
                                  ctx) != RET_OK) {
        return {false, {}, "Failed to send download manifest command."};
    }
    return {true, {}, ""};
}

// ---------------------------------------------------------------------------
// importFiles (headless helper)
// ---------------------------------------------------------------------------

void StorageModuleImpl::importFiles(const std::string& path) {
    fprintf(stderr, "StorageModuleImpl::importFiles from path=%s\n",
            path.c_str());
    std::error_code ec;
    if (!fs::is_directory(path, ec)) {
        fprintf(stderr,
                "StorageModuleImpl::importFiles: not a directory: %s\n",
                path.c_str());
        return;
    }
    for (const auto& entry : fs::directory_iterator(path, ec)) {
        if (!entry.is_regular_file()) continue;
        std::string fp = entry.path().string();
        fprintf(stderr, "StorageModuleImpl::importFiles: uploading %s\n",
                fp.c_str());
        StdLogosResult result = uploadUrl(fp, DEFAULT_CHUNK_SIZE);
        if (!result.success) {
            fprintf(stderr,
                    "StorageModuleImpl::importFiles: failed to start upload "
                    "for %s\n",
                    fp.c_str());
        } else {
            std::string sid = result.value.is_string()
                                  ? result.value.get<std::string>()
                                  : std::string();
            fprintf(stderr,
                    "StorageModuleImpl::importFiles: upload started, "
                    "session=%s\n",
                    sid.c_str());
        }
    }
}
