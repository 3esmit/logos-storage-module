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
static constexpr int DOWNLOAD_CHUNK_TIMEOUT_MS = 60000;
static constexpr int MAX_DOWNLOAD_V2_BYTES = 1073741824;
static constexpr size_t MAX_TERMINAL_DOWNLOADS_V2 = 128;
static std::atomic<uint64_t> nextDownloadV2StagingFileId{0};

static std::string fromMsg(const char* msg, size_t len) {
    return (msg && len > 0) ? std::string(msg, len) : std::string();
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

struct SyncResult {
    bool ok = false;
    std::string message;
};

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

    SimpleEventCtx(StorageModuleImpl* i, StorageEvent ev)
        : impl(i), event(ev) {}

    void handleResponse(int ret, const char* msg, size_t len) override {
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
    int64_t totalBytes;
    mutable int64_t bytesDownloaded = 0;
    mutable int64_t pendingBytes = 0;
    mutable int lastEmittedPercent = -1;

    DownloadStreamCtx(StorageModuleImpl* i, std::string c, std::string fp,
                      int64_t total = 0)
        : impl(i), cid(std::move(c)), filepath(std::move(fp)),
          totalBytes(total) {}

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

// A V2 transfer uses one libstorage chunk request at a time. This keeps the
// storage worker free between chunks, which is necessary because libstorage
// cannot cancel a stream-mode download while its worker is busy.
struct DownloadV2State {
    std::string cid;
    std::string operationId;
    std::mutex mutex;
    std::condition_variable cancellationReady;
    bool cancellationRequested = false;
    bool cancellationDispatched = false;
    bool cancellationResolved = false;
    bool cancellationSucceeded = false;
    std::string cancellationError;
    std::atomic<bool> workerFinished{false};
};

struct DownloadV2CancelCtx {
    std::shared_ptr<DownloadV2State> state;
};

static void downloadV2CancelCallback(int ret, const char* msg, size_t len,
                                     void* userData) {
    if (!userData || ret == RET_PROGRESS) return;
    auto* ctx = static_cast<DownloadV2CancelCtx*>(userData);
    {
        std::lock_guard<std::mutex> lock(ctx->state->mutex);
        ctx->state->cancellationResolved = true;
        ctx->state->cancellationSucceeded = ret == RET_OK;
        ctx->state->cancellationError = ret == RET_OK
            ? std::string()
            : fromMsg(msg, len);
    }
    ctx->state->cancellationReady.notify_all();
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
        delete ctx;
        resolveDispatchFailure("Failed to send download cancellation.");
        return false;
    }
    return true;
}

static SyncResult waitForDownloadV2Cancellation(
    const std::shared_ptr<DownloadV2State>& state) {
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->cancellationReady.wait_for(
            lock, std::chrono::milliseconds(DOWNLOAD_CANCEL_TIMEOUT_MS),
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
using StorageDownloadInitFn =
    int (*)(void*, const char*, size_t, bool, StorageCallback, void*);

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

static SyncResult syncCallDownloadInit(void* ctx, StorageDownloadInitFn fn,
                                        const std::string& cid, size_t chunkSize,
                                        bool local, int timeoutMs) {
    if (!ctx) return {false, "Storage context not initialized."};
    auto* sctx = new SyncCtx();
    sctx->lifetimeArg = cid;
    if (fn(ctx, sctx->lifetimeArg.c_str(), chunkSize, local, syncCallback, sctx) !=
        RET_OK) {
        delete sctx;
        return {false, "Failed to send command."};
    }
    return waitSync(sctx, timeoutMs);
}

// ---------------------------------------------------------------------------
// StorageModuleImpl
// ---------------------------------------------------------------------------

StorageModuleImpl::StorageModuleImpl() : storageCtx(nullptr) {
    fprintf(stderr, "StorageModuleImpl: Initializing...\n");
}

StorageModuleImpl::~StorageModuleImpl() {
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

bool StorageModuleImpl::init(const std::string& cfg) {
    fprintf(stderr, "StorageModuleImpl::init called\n");

    if (storageCtx) {
        fprintf(stderr, "StorageModuleImpl::init: context already initialized\n");
        return false;
    }

    auto* sctx = new SyncCtx();
    storageCtx = storage_new(cfg.c_str(), syncCallback, sctx);
    SyncResult r = waitSync(sctx, 1000);

    if (!r.ok || !storageCtx) {
        fprintf(stderr, "StorageModuleImpl::init failed: %s\n",
                r.message.c_str());
        storageCtx = nullptr;
        return false;
    }
    return true;
}

bool StorageModuleImpl::start() {
    fprintf(stderr, "StorageModuleImpl::start called\n");
    if (!storageCtx) {
        fprintf(stderr, "StorageModuleImpl::start: context not initialized\n");
        return false;
    }
    auto* ctx = new SimpleEventCtx(this, &StorageModuleImpl::storageStart);
    if (storage_start(storageCtx, asyncCallback, ctx) != RET_OK) {
        delete ctx;
        return false;
    }
    return true;
}

StdLogosResult StorageModuleImpl::stop() {
    fprintf(stderr, "StorageModuleImpl::stop called\n");
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    auto* ctx = new SimpleEventCtx(this, &StorageModuleImpl::storageStop);
    if (storage_stop(storageCtx, asyncCallback, ctx) != RET_OK) {
        delete ctx;
        return {false, {}, "Failed to send stop command."};
    }
    return {true, {}, ""};
}

StdLogosResult StorageModuleImpl::destroy() {
    fprintf(stderr, "StorageModuleImpl::destroy called\n");
    if (!storageCtx)
        return {false, {}, "Storage context not initialized."};
    cancelAndJoinDownloadV2Workers();
    syncCallNoArg(storageCtx, storage_close, 1000);
    int ret = storage_destroy(storageCtx);
    if (ret == RET_OK) {
        storageCtx = nullptr;
        return {true, {}, ""};
    }
    return {false, {}, "Failed to destroy storage context."};
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
        return {true, json::parse(r.message), ""};
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
        delete ctx;
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
        delete ctx;
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
        delete ctx;
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
    if (!storageCtx || chunkSize <= 0) return {};

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
            return {};
        }
    }

    auto r = syncCallDownloadInit(storageCtx, storage_download_init, cid,
                                  static_cast<size_t>(chunkSize), local, 1000);
    if (!r.ok) return {};

    auto* ctx = new DownloadStreamCtx(this, cid, filepath, totalBytes);
    if (storage_download_stream(storageCtx, ctx->cid.c_str(),
                                static_cast<size_t>(chunkSize), local,
                                ctx->filepath.c_str(),
                                asyncCallback, ctx) != RET_OK) {
        delete ctx;
        syncCallString(storageCtx, storage_download_cancel, cid, 1000);
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
    };
}

StdLogosResult StorageModuleImpl::downloadToUrlV2(
    const std::string& cid, const std::string& filePath, bool local,
    int chunkSize, const std::string& operationId, int maxDownloadBytes) {
    reapFinishedDownloadV2Workers();
    if (!storageCtx) {
        return {false, {}, "Storage context not initialized."};
    }
    if (cid.empty() || filePath.empty() || operationId.empty()
        || operationId.find_first_of(" \t\r\n") != std::string::npos || operationId == cid
        || chunkSize <= 0 || maxDownloadBytes <= 0
        || maxDownloadBytes > MAX_DOWNLOAD_V2_BYTES) {
        return {false, {}, "Invalid versioned download arguments."};
    }

    {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        if (pendingDownloadOperationIdsV2.find(operationId)
                != pendingDownloadOperationIdsV2.end()
            || activeDownloadsV2.find(operationId) != activeDownloadsV2.end()
            || terminalDownloadsV2.find(operationId) != terminalDownloadsV2.end()) {
            return {false, {}, "Download operation ID is already in use."};
        }
        if (pendingDownloadCidsV2.find(cid) != pendingDownloadCidsV2.end()) {
            return {false, {}, "A download for this CID is already starting."};
        }
        for (const auto& active : activeDownloadsV2) {
            if (active.second.cid == cid) {
                return {false, {}, "A download for this CID is already active."};
            }
        }
        pendingDownloadOperationIdsV2.insert(operationId);
        pendingDownloadCidsV2.insert(cid);
    }

    const auto manifest = syncCallString(storageCtx, storage_download_manifest, cid, 3000);
    uint64_t manifestBytes = 0;
    if (!manifest.ok || !manifestDatasetSize(manifest.message, manifestBytes)) {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        pendingDownloadOperationIdsV2.erase(operationId);
        pendingDownloadCidsV2.erase(cid);
        return {false, {}, "Failed to read download manifest."};
    }
    if (manifestBytes > static_cast<uint64_t>(maxDownloadBytes)) {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        pendingDownloadOperationIdsV2.erase(operationId);
        pendingDownloadCidsV2.erase(cid);
        return {false, {}, "Download exceeds requested byte limit."};
    }

    const fs::path stagingPath = downloadV2StagingPath(filePath);
    {
        std::ofstream probe(stagingPath, std::ios::binary | std::ios::trunc);
        if (!probe) {
            std::lock_guard<std::mutex> lock(downloadV2Mutex);
            pendingDownloadOperationIdsV2.erase(operationId);
            pendingDownloadCidsV2.erase(cid);
            return {false, {}, "Failed to open download staging file."};
        }
    }

    const auto initialized = syncCallDownloadInit(
        storageCtx, storage_download_init, cid, static_cast<size_t>(chunkSize), local, 1000);
    if (!initialized.ok) {
        std::error_code ec;
        fs::remove(stagingPath, ec);
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        pendingDownloadOperationIdsV2.erase(operationId);
        pendingDownloadCidsV2.erase(cid);
        return {false, {}, initialized.message};
    }

    auto state = std::make_shared<DownloadV2State>();
    state->cid = cid;
    state->operationId = operationId;
    {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        pendingDownloadOperationIdsV2.erase(operationId);
        pendingDownloadCidsV2.erase(cid);
        activeDownloadsV2.emplace(operationId, ActiveDownloadV2{cid, state});
    }

    try {
        std::lock_guard<std::mutex> lock(downloadV2WorkersMutex);
        downloadV2Workers.reserve(downloadV2Workers.size() + 1);
        std::thread worker(&StorageModuleImpl::runDownloadV2, this, state,
                           stagingPath.string(), filePath, chunkSize, manifestBytes,
                           static_cast<uint64_t>(maxDownloadBytes));
        downloadV2Workers.push_back({state, std::move(worker)});
    } catch (const std::exception&) {
        {
            std::lock_guard<std::mutex> lock(downloadV2Mutex);
            activeDownloadsV2.erase(operationId);
        }
        syncCallString(storageCtx, storage_download_cancel, cid, 1000);
        std::error_code ec;
        fs::remove(stagingPath, ec);
        return {false, {}, "Failed to start versioned download worker."};
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

    std::string cid;
    std::shared_ptr<DownloadV2State> state;
    {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        const auto active = activeDownloadsV2.find(operationId);
        if (active == activeDownloadsV2.end()) {
            const auto terminal = terminalDownloadsV2.find(operationId);
            if (terminal == terminalDownloadsV2.end()) {
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
        cid = active->second.cid;
        state = active->second.state;
    }

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->cancellationRequested = true;
    }
    if (!requestDownloadV2Cancellation(storageCtx, state)) {
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
    const std::string& destinationPath, int chunkSize, uint64_t expectedBytes,
    uint64_t maxBytes) {
    std::ofstream output(stagingPath, std::ios::binary | std::ios::trunc);
    auto removePartialFile = [&stagingPath] {
        std::error_code ec;
        fs::remove(stagingPath, ec);
    };
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
            finishDownloadV2(state, "failed", canceled.message);
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
        finishDownloadV2(state, "failed", failure);
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
            fail(chunk.error.empty() ? "Storage download chunk failed." : chunk.error, false);
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
                finishDownloadV2(state, "failed", cleanup.message);
                return;
            }
            if (cancellationRequested()) {
                removePartialFile();
                finishDownloadV2(state, "canceled");
                return;
            }
            std::error_code renameError;
            fs::rename(stagingPath, destinationPath, renameError);
            if (renameError) {
                removePartialFile();
                finishDownloadV2(
                    state, "failed",
                    "Failed to replace download destination: " + renameError.message());
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
    const std::string& error) {
    {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        activeDownloadsV2.erase(state->operationId);
        terminalDownloadsV2[state->operationId] = {state->cid, outcome};
        terminalDownloadOrderV2.push_back(state->operationId);
        while (terminalDownloadOrderV2.size() > MAX_TERMINAL_DOWNLOADS_V2) {
            const std::string expired = terminalDownloadOrderV2.front();
            terminalDownloadOrderV2.pop_front();
            terminalDownloadsV2.erase(expired);
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
    std::vector<std::shared_ptr<DownloadV2State>> active;
    {
        std::lock_guard<std::mutex> lock(downloadV2Mutex);
        for (const auto& entry : activeDownloadsV2) {
            active.push_back(entry.second.state);
        }
    }
    for (const auto& state : active) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->cancellationRequested = true;
        }
        requestDownloadV2Cancellation(storageCtx, state);
    }

    std::vector<DownloadV2Worker> workers;
    {
        std::lock_guard<std::mutex> lock(downloadV2WorkersMutex);
        workers.swap(downloadV2Workers);
    }
    for (DownloadV2Worker& worker : workers) {
        if (worker.thread.joinable()) worker.thread.join();
    }
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
        delete ctx;
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
        delete ctx;
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
