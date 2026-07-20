// Unit tests for StorageModuleImpl.
// All libstorage C functions are mocked at link time via mock_libstorage.cpp.
// Async mocks invoke the callback immediately so the condvar is signalled
// before waitSync's first check.

#include <logos_test.h>
#include "storage_module_plugin.h"
#include "mocks/mock_libstorage_control.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <nlohmann/json.hpp>
using json = nlohmann::json;
namespace fs = std::filesystem;

// Helper: create an impl with a mocked, successfully initialized storage context.
static StorageModuleImpl* createInitializedImpl(LogosTestContext& t) {
    t.mockCFunction("storage_new").returns(1);
    auto* impl = new StorageModuleImpl();
    LOGOS_ASSERT_TRUE(impl->init("{\"data-dir\":\"/tmp/test\"}"));
    return impl;
}

// init

LOGOS_TEST(init_succeeds_when_storage_new_returns_context) {
    auto t = LogosTestContext("storage_module");
    t.mockCFunction("storage_new").returns(1);

    StorageModuleImpl impl;
    LOGOS_ASSERT_TRUE(impl.init("{\"data-dir\":\"/tmp/test\"}"));
    LOGOS_ASSERT(t.cFunctionCalled("storage_new"));
}

LOGOS_TEST(init_fails_when_storage_new_returns_null) {
    auto t = LogosTestContext("storage_module");
    t.mockCFunction("storage_new").returns(0);

    StorageModuleImpl impl;
    LOGOS_ASSERT_FALSE(impl.init("{\"data-dir\":\"/tmp/test\"}"));
}

// version

LOGOS_TEST(version_returns_mocked_string) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_version").returns("1.2.3-test");
    StdLogosResult vr = impl->version();

    LOGOS_ASSERT_TRUE(vr.success);
    LOGOS_ASSERT_EQ(vr.value.get<std::string>(), std::string("1.2.3-test"));
    LOGOS_ASSERT(t.cFunctionCalled("storage_version"));

    impl->destroy();
    delete impl;
}

// The value is injected at build time from metadata.json; the fallback
// "0.0.0-dev" only survives when the build failed to pass the define, so
// asserting against it verifies the injection pipeline actually ran.
LOGOS_TEST(moduleVersion_is_injected_from_build) {
    StorageModuleImpl impl;
    std::string v = impl.moduleVersion();

    LOGOS_ASSERT_FALSE(v.empty());
    LOGOS_ASSERT(v != std::string("0.0.0-dev"));
}

// start / stop

LOGOS_TEST(start_returns_true_after_init) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->start());
    LOGOS_ASSERT(t.cFunctionCalled("storage_start"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(start_returns_false_without_init) {
    auto t = LogosTestContext("storage_module");
    StorageModuleImpl impl;
    LOGOS_ASSERT_FALSE(impl.start());
}

LOGOS_TEST(stop_fails_without_init) {
    auto t = LogosTestContext("storage_module");
    StorageModuleImpl impl;
    LOGOS_ASSERT_FALSE(impl.stop().success);
}

LOGOS_TEST(stop_succeeds_after_init) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->stop().success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_stop"));

    impl->destroy();
    delete impl;
}

// destroy

LOGOS_TEST(destroy_without_init_returns_error) {
    auto t = LogosTestContext("storage_module");
    StorageModuleImpl impl;
    // destroy() must not touch the C API when there is no context
    LOGOS_ASSERT_FALSE(impl.destroy().success);
    LOGOS_ASSERT(!t.cFunctionCalled("storage_destroy"));
}

LOGOS_TEST(destroy_succeeds_after_init) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->destroy().success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_close"));
    LOGOS_ASSERT(t.cFunctionCalled("storage_destroy"));

    delete impl;
}

// peerId / spr / dataDir

LOGOS_TEST(peerId_returns_mocked_value) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_peer_id").returns("QmTestPeerId123");
    StdLogosResult r = impl->peerId();

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_EQ(r.value.get<std::string>(), std::string("QmTestPeerId123"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(spr_returns_mocked_value) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_spr").returns("spr:ABCD1234");
    StdLogosResult r = impl->spr();

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_EQ(r.value.get<std::string>(), std::string("spr:ABCD1234"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(dataDir_returns_mocked_value) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_repo").returns("/tmp/test-data");
    StdLogosResult r = impl->dataDir();

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_EQ(r.value.get<std::string>(), std::string("/tmp/test-data"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(peerId_returns_failure_without_init) {
    auto t = LogosTestContext("storage_module");
    StorageModuleImpl impl;
    LOGOS_ASSERT_FALSE(impl.peerId().success);
}

// debug

LOGOS_TEST(debug_returns_parsed_map) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_debug")
        .returns(R"({"id":"QmNode","addrs":[],"announceAddresses":[],"table":{}})");
    StdLogosResult r = impl->debug();

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_TRUE(r.value.is_object());
    LOGOS_ASSERT_FALSE(r.value.empty());
    LOGOS_ASSERT_TRUE(r.value.contains("id"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(debug_fails_on_invalid_json) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_debug").returns("not json");
    StdLogosResult r = impl->debug();
    LOGOS_ASSERT_FALSE(r.success);

    impl->destroy();
    delete impl;
}

// collectMetrics

LOGOS_TEST(collectMetrics_returns_parsed_metrics) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_get_metrics")
        .returns(R"({"metrics":[{"name":"test_metric","type":"gauge","help":"Test metric","value":1.0,"labels":{}}]})");
    LogosMap r = impl->collectMetrics();

    LOGOS_ASSERT_TRUE(r.is_object());
    LOGOS_ASSERT_TRUE(r.contains("metrics"));
    LOGOS_ASSERT_TRUE(r["metrics"].is_array());
    LOGOS_ASSERT_EQ(static_cast<int>(r["metrics"].size()), 1);
    LOGOS_ASSERT(t.cFunctionCalled("storage_get_metrics"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(collectMetrics_returns_empty_metrics_on_libstorage_error) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_get_metrics").returns(1);
    LogosMap r = impl->collectMetrics();

    LOGOS_ASSERT_TRUE(r.is_object());
    LOGOS_ASSERT_TRUE(r.contains("metrics"));
    LOGOS_ASSERT_TRUE(r["metrics"].is_array());
    LOGOS_ASSERT_TRUE(r["metrics"].empty());

    impl->destroy();
    delete impl;
}

LOGOS_TEST(collectMetrics_returns_empty_metrics_on_invalid_json) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_get_metrics").returns("not json");
    LogosMap r = impl->collectMetrics();

    LOGOS_ASSERT_TRUE(r.is_object());
    LOGOS_ASSERT_TRUE(r.contains("metrics"));
    LOGOS_ASSERT_TRUE(r["metrics"].is_array());
    LOGOS_ASSERT_TRUE(r["metrics"].empty());

    impl->destroy();
    delete impl;
}

LOGOS_TEST(collectMetrics_returns_empty_metrics_when_payload_is_array) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_get_metrics").returns(R"([])");
    LogosMap r = impl->collectMetrics();

    LOGOS_ASSERT_TRUE(r.is_object());
    LOGOS_ASSERT_TRUE(r.contains("metrics"));
    LOGOS_ASSERT_TRUE(r["metrics"].is_array());
    LOGOS_ASSERT_TRUE(r["metrics"].empty());

    impl->destroy();
    delete impl;
}

LOGOS_TEST(collectMetrics_returns_empty_metrics_when_metrics_missing) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_get_metrics").returns(R"({"other":[]})");
    LogosMap r = impl->collectMetrics();

    LOGOS_ASSERT_TRUE(r.is_object());
    LOGOS_ASSERT_TRUE(r.contains("metrics"));
    LOGOS_ASSERT_TRUE(r["metrics"].is_array());
    LOGOS_ASSERT_TRUE(r["metrics"].empty());

    impl->destroy();
    delete impl;
}

LOGOS_TEST(collectMetrics_returns_empty_metrics_when_metrics_is_not_array) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_get_metrics").returns(R"({"metrics":{}})");
    LogosMap r = impl->collectMetrics();

    LOGOS_ASSERT_TRUE(r.is_object());
    LOGOS_ASSERT_TRUE(r.contains("metrics"));
    LOGOS_ASSERT_TRUE(r["metrics"].is_array());
    LOGOS_ASSERT_TRUE(r["metrics"].empty());

    impl->destroy();
    delete impl;
}

// updateLogLevel

LOGOS_TEST(updateLogLevel_returns_true_on_success) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->updateLogLevel("DEBUG").success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_log_level"));

    impl->destroy();
    delete impl;
}

// exists

LOGOS_TEST(exists_returns_true_when_cid_found) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_exists").returns("true");
    StdLogosResult r = impl->exists("QmSomeCid");
    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_TRUE(r.value.get<bool>());

    impl->destroy();
    delete impl;
}

LOGOS_TEST(exists_returns_false_when_cid_not_found) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_exists").returns("false");
    StdLogosResult r = impl->exists("QmMissingCid");
    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_FALSE(r.value.get<bool>());

    impl->destroy();
    delete impl;
}

// togglePrivateQueries

LOGOS_TEST(togglePrivateQueries_returns_previous_state) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_toggle_private_queries").returns("false");
    StdLogosResult r = impl->togglePrivateQueries(true);
    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_FALSE(r.value.get<bool>());
    LOGOS_ASSERT(t.cFunctionCalled("storage_toggle_private_queries"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(togglePrivateQueries_maps_true_previous_state) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_toggle_private_queries").returns("true");
    StdLogosResult r = impl->togglePrivateQueries(false);
    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_TRUE(r.value.get<bool>());

    impl->destroy();
    delete impl;
}

// fetch / remove

LOGOS_TEST(fetch_calls_storage_fetch) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->fetch("QmSomeCid").success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_fetch"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(remove_dispatches_and_emits_event) {
    logos_test::EventCapture events;
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->remove("QmSomeCid").success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_delete"));
    LOGOS_ASSERT_TRUE(events.has("storageRemoveDone"));

    impl->destroy();
    delete impl;
}

// space

LOGOS_TEST(space_returns_parsed_map) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_space")
        .returns(R"({"totalBlocks":100,"quotaMaxBytes":1000,"quotaUsedBytes":50,"quotaReservedBytes":10})");
    StdLogosResult r = impl->space();

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_TRUE(r.value.is_object());
    LOGOS_ASSERT_FALSE(r.value.empty());
    LOGOS_ASSERT_TRUE(r.value.contains("totalBlocks"));
    LOGOS_ASSERT_TRUE(r.value.contains("quotaMaxBytes"));

    impl->destroy();
    delete impl;
}

// manifests

LOGOS_TEST(manifests_returns_parsed_list) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_list")
        .returns(R"([{"cid":"QmABC","manifest":{"treeCid":"QmTree","datasetSize":1024,"blockSize":64,"filename":"test.txt","mimetype":"text/plain"}}])");
    StdLogosResult r = impl->manifests();

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_TRUE(r.value.is_array());
    LOGOS_ASSERT_EQ(static_cast<int>(r.value.size()), 1);

    impl->destroy();
    delete impl;
}

// downloadManifest

LOGOS_TEST(downloadManifest_dispatches_and_emits_event) {
    logos_test::EventCapture events;
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_download_manifest")
        .returns(R"({"treeCid":"QmTree","datasetSize":2048,"blockSize":64,"filename":"data.bin","mimetype":"application/octet-stream"})");
    StdLogosResult r = impl->downloadManifest("QmSomeCid");

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_TRUE(events.has("storageDownloadManifestDone"));

    impl->destroy();
    delete impl;
}

// uploadInit / uploadFinalize / uploadCancel

LOGOS_TEST(uploadInit_returns_session_id) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_upload_init").returns("session-abc-123");
    StdLogosResult r = impl->uploadInit("test.txt", 65536);

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_EQ(r.value.get<std::string>(), std::string("session-abc-123"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(uploadFinalize_returns_cid) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    t.mockCFunction("storage_upload_finalize").returns("QmFinalCid");
    StdLogosResult r = impl->uploadFinalize("session-abc-123");

    LOGOS_ASSERT_TRUE(r.success);
    LOGOS_ASSERT_EQ(r.value.get<std::string>(), std::string("QmFinalCid"));
    LOGOS_ASSERT(t.cFunctionCalled("storage_upload_finalize"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(uploadCancel_returns_true) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->uploadCancel("session-abc-123").success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_upload_cancel"));

    impl->destroy();
    delete impl;
}

// uploadUrl input validation

LOGOS_TEST(uploadUrl_fails_with_nonexistent_file) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    StdLogosResult r = impl->uploadUrl("/nonexistent/path/file.txt", 65536);
    LOGOS_ASSERT_FALSE(r.success);

    impl->destroy();
    delete impl;
}

LOGOS_TEST(uploadUrl_fails_with_zero_chunk_size) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    StdLogosResult r = impl->uploadUrl("/tmp/test.txt", 0);
    LOGOS_ASSERT_FALSE(r.success);

    impl->destroy();
    delete impl;
}

// downloadCancel

LOGOS_TEST(downloadCancel_returns_true) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->downloadCancel("QmSomeCid").success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_download_cancel"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(downloadChunks_cancels_session_when_stream_fails) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    // storage_download_init succeeds, but the stream dispatch fails: the
    // already-open session must be cancelled and the call must report failure.
    t.mockCFunction("storage_download_stream").returns(1);
    StdLogosResult r = impl->downloadChunks("QmSomeCid", false, 65536);
    LOGOS_ASSERT_FALSE(r.success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_download_cancel"));

    impl->destroy();
    delete impl;
}

// Versioned downloads used by callers that need an authoritative terminal
// event and caller-owned operation identity.

LOGOS_TEST(downloadProtocol_reports_v2_contract) {
    StorageModuleImpl impl;

    const LogosMap protocol = impl.downloadProtocol();

    LOGOS_ASSERT_EQ(protocol.at("protocol").get<std::string>(),
                    std::string("logos.storage.download"));
    LOGOS_ASSERT_EQ(protocol.at("version").get<int>(), 2);
    LOGOS_ASSERT_EQ(protocol.at("moduleOperationIdOwner").get<std::string>(),
                    std::string("caller"));
    LOGOS_ASSERT_EQ(protocol.at("cancelTimeoutMs").get<int>(), 15000);
    LOGOS_ASSERT_EQ(protocol.at("maxDownloadBytes").get<int>(), 1073741824);
}

LOGOS_TEST(downloadToUrlV2_acknowledges_and_emits_correlated_terminal_event) {
    auto t = LogosTestContext("storage_module");
    logos_test::EventCapture events;
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":32})");
    constexpr char kPayload[] = "0123456789abcdef0123456789abcdef";
    const std::string path = "/tmp/logos-storage-v2-success";
    {
        std::ofstream existing(path, std::ios::binary | std::ios::trunc);
        existing << "existing backup";
    }
    mockStorageSetNextDownloadChunkPayload(kPayload);

    const StdLogosResult result = impl->downloadToUrlV2(
        "QmVersionedCid", path, false, 65536,
        "download-operation-1", 64);

    LOGOS_ASSERT_TRUE(result.success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_download_init"));
    LOGOS_ASSERT_EQ(result.value.at("moduleOperationId").get<std::string>(),
                    std::string("download-operation-1"));
    const auto event = events.waitFor("storageDownloadDoneV2", 1000);
    LOGOS_ASSERT_EQ(event.name, std::string("storageDownloadDoneV2"));
    const json terminal = json::parse(event.data);
    LOGOS_ASSERT_EQ(terminal.at("protocol").get<std::string>(),
                    std::string("logos.storage.download"));
    LOGOS_ASSERT_EQ(terminal.at("version").get<int>(), 2);
    LOGOS_ASSERT_EQ(terminal.at("moduleOperationId").get<std::string>(),
                    std::string("download-operation-1"));
    LOGOS_ASSERT_EQ(terminal.at("cid").get<std::string>(),
                    std::string("QmVersionedCid"));
    LOGOS_ASSERT_EQ(terminal.at("outcome").get<std::string>(),
                    std::string("succeeded"));
    LOGOS_ASSERT(t.cFunctionCalled("storage_download_cancel"));

    std::ifstream output(path, std::ios::binary);
    const std::string downloaded((std::istreambuf_iterator<char>(output)),
                                 std::istreambuf_iterator<char>());
    LOGOS_ASSERT_EQ(downloaded, std::string(kPayload));

    const StdLogosResult cancel = impl->downloadCancelV2("download-operation-1");
    LOGOS_ASSERT_TRUE(cancel.success);
    LOGOS_ASSERT_EQ(cancel.value.at("cancelStatus").get<std::string>(),
                    std::string("already_terminal"));
    LOGOS_ASSERT_EQ(cancel.value.at("terminalOutcome").get<std::string>(),
                    std::string("succeeded"));

    impl->destroy();
    delete impl;
    fs::remove(path);
}

LOGOS_TEST(downloadToUrlV2_rejects_oversized_manifest_before_chunk_dispatch) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":65})");

    const StdLogosResult result = impl->downloadToUrlV2(
        "QmOversizedCid", "/tmp/versioned-download", false, 65536,
        "download-operation-oversized", 64);

    LOGOS_ASSERT_FALSE(result.success);
    LOGOS_ASSERT_FALSE(t.cFunctionCalled("storage_download_init"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(downloadToUrlV2_cancel_reports_canceled_terminal_outcome) {
    auto t = LogosTestContext("storage_module");
    logos_test::EventCapture events;
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":32})");
    const std::string path = "/tmp/logos-storage-v2-canceled";
    {
        std::ofstream existing(path, std::ios::binary | std::ios::trunc);
        existing << "existing backup";
    }
    mockStorageHoldNextDownloadChunk();

    const StdLogosResult start = impl->downloadToUrlV2(
        "QmCancelableCid", path, false, 65536,
        "download-operation-cancel", 64);
    LOGOS_ASSERT_TRUE(start.success);
    LOGOS_ASSERT(mockStorageWaitForHeldDownloadChunk(1000));
    const StdLogosResult cancel = impl->downloadCancelV2("download-operation-cancel");
    LOGOS_ASSERT_TRUE(cancel.success);
    LOGOS_ASSERT_EQ(cancel.value.at("cancelStatus").get<std::string>(),
                    std::string("canceled"));
    LOGOS_ASSERT(t.cFunctionCalled("storage_download_cancel"));

    mockStorageCompleteHeldDownloadChunk(RET_OK, nullptr, nullptr);
    const auto event = events.waitFor("storageDownloadDoneV2", 1000);
    LOGOS_ASSERT_EQ(event.name, std::string("storageDownloadDoneV2"));
    const json terminal = json::parse(event.data);
    LOGOS_ASSERT_EQ(terminal.at("outcome").get<std::string>(),
                    std::string("canceled"));
    LOGOS_ASSERT_FALSE(terminal.contains("error"));

    std::ifstream destination(path, std::ios::binary);
    const std::string contents((std::istreambuf_iterator<char>(destination)),
                               std::istreambuf_iterator<char>());
    LOGOS_ASSERT_EQ(contents, std::string("existing backup"));

    impl->destroy();
    delete impl;
    fs::remove(path);
}

LOGOS_TEST(downloadToUrlV2_rejects_a_second_active_session_for_the_same_cid) {
    auto t = LogosTestContext("storage_module");
    logos_test::EventCapture events;
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":32})");
    const std::string firstPath = "/tmp/logos-storage-v2-first";
    const std::string secondPath = "/tmp/logos-storage-v2-second";
    fs::remove(firstPath);
    fs::remove(secondPath);
    mockStorageHoldNextDownloadChunk();

    const StdLogosResult first = impl->downloadToUrlV2(
        "QmSharedCid", firstPath, false, 65536, "download-operation-first", 64);
    LOGOS_ASSERT_TRUE(first.success);
    LOGOS_ASSERT(mockStorageWaitForHeldDownloadChunk(1000));

    const StdLogosResult second = impl->downloadToUrlV2(
        "QmSharedCid", secondPath, false, 65536, "download-operation-second", 64);
    LOGOS_ASSERT_FALSE(second.success);
    LOGOS_ASSERT_EQ(second.error, std::string("A download for this CID is already active."));

    LOGOS_ASSERT_TRUE(impl->downloadCancelV2("download-operation-first").success);
    mockStorageCompleteHeldDownloadChunk(RET_OK, nullptr, nullptr);
    const auto event = events.waitFor("storageDownloadDoneV2", 1000);
    LOGOS_ASSERT_EQ(event.name, std::string("storageDownloadDoneV2"));

    impl->destroy();
    delete impl;
    fs::remove(firstPath);
    fs::remove(secondPath);
}

LOGOS_TEST(downloadToUrlV2_reports_terminal_failure_after_chunk_dispatch_failure) {
    auto t = LogosTestContext("storage_module");
    logos_test::EventCapture events;
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":32})");
    t.mockCFunction("storage_download_chunk").returns(1);
    const std::string path = "/tmp/logos-storage-v2-failure";
    fs::remove(path);

    const StdLogosResult start = impl->downloadToUrlV2(
        "QmFailedCid", path, false, 65536,
        "download-operation-failed", 64);
    LOGOS_ASSERT_TRUE(start.success);
    const auto event = events.waitFor("storageDownloadDoneV2", 1000);
    LOGOS_ASSERT_EQ(event.name, std::string("storageDownloadDoneV2"));
    const json terminal = json::parse(event.data);
    LOGOS_ASSERT_EQ(terminal.at("outcome").get<std::string>(),
                    std::string("failed"));
    LOGOS_ASSERT_TRUE(terminal.contains("error"));
    const StdLogosResult cancel = impl->downloadCancelV2("download-operation-failed");
    LOGOS_ASSERT_TRUE(cancel.success);
    LOGOS_ASSERT_EQ(cancel.value.at("cancelStatus").get<std::string>(),
                    std::string("already_terminal"));

    impl->destroy();
    delete impl;
    fs::remove(path);
}

LOGOS_TEST(downloadToUrlV2_preserves_existing_destination_after_failure) {
    auto t = LogosTestContext("storage_module");
    logos_test::EventCapture events;
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":32})");
    t.mockCFunction("storage_download_chunk").returns(1);
    const std::string path = "/tmp/logos-storage-v2-preserved-destination";
    {
        std::ofstream existing(path, std::ios::binary | std::ios::trunc);
        existing << "existing backup";
    }

    const StdLogosResult start = impl->downloadToUrlV2(
        "QmPreservedCid", path, false, 65536,
        "download-operation-preserve", 64);
    LOGOS_ASSERT_TRUE(start.success);
    const auto event = events.waitFor("storageDownloadDoneV2", 1000);
    LOGOS_ASSERT_EQ(event.name, std::string("storageDownloadDoneV2"));
    LOGOS_ASSERT_EQ(json::parse(event.data).at("outcome").get<std::string>(),
                    std::string("failed"));

    std::ifstream destination(path, std::ios::binary);
    const std::string contents((std::istreambuf_iterator<char>(destination)),
                               std::istreambuf_iterator<char>());
    LOGOS_ASSERT_EQ(contents, std::string("existing backup"));

    impl->destroy();
    delete impl;
    fs::remove(path);
}

LOGOS_TEST(downloadToUrlV2_enforces_byte_limit_during_chunk_copy) {
    auto t = LogosTestContext("storage_module");
    logos_test::EventCapture events;
    auto* impl = createInitializedImpl(t);
    t.mockCFunction("storage_download_manifest").returns(R"({"datasetSize":64})");
    const std::string path = "/tmp/logos-storage-v2-limit";
    fs::remove(path);
    const std::string oversized(65, 'x');
    mockStorageSetNextDownloadChunkPayload(oversized.c_str());

    const StdLogosResult start = impl->downloadToUrlV2(
        "QmLimitedCid", path, false, 64, "download-operation-limit", 64);
    LOGOS_ASSERT_TRUE(start.success);
    const auto event = events.waitFor("storageDownloadDoneV2", 1000);
    LOGOS_ASSERT_EQ(event.name, std::string("storageDownloadDoneV2"));
    const json terminal = json::parse(event.data);
    LOGOS_ASSERT_EQ(terminal.at("outcome").get<std::string>(),
                    std::string("failed"));
    LOGOS_ASSERT_EQ(terminal.at("error").get<std::string>(),
                    std::string("Download exceeded requested byte limit."));
    LOGOS_ASSERT_FALSE(fs::exists(path));

    impl->destroy();
    delete impl;
}

// connect

LOGOS_TEST(connect_fails_without_init) {
    auto t = LogosTestContext("storage_module");
    StorageModuleImpl impl;
    LOGOS_ASSERT_FALSE(impl.connect("QmPeer", {"addr1"}).success);
}

LOGOS_TEST(connect_succeeds_after_init) {
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->connect("QmPeer", {"/ip4/127.0.0.1/tcp/1234"}).success);
    LOGOS_ASSERT(t.cFunctionCalled("storage_connect"));

    impl->destroy();
    delete impl;
}

// Event wiring — typed `logos_events:` methods are forwarded to
// logos_test::recordEvent by tests/storage_events_test.cpp, so tests observe
// them via EventCapture. Construct the capture before the impl so it outlives
// any background thread the impl's destructor joins.

LOGOS_TEST(start_emits_storageStart_event) {
    logos_test::EventCapture events;
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->start());
    LOGOS_ASSERT_TRUE(events.has("storageStart"));

    impl->destroy();
    delete impl;
}

LOGOS_TEST(stop_emits_storageStop_event) {
    logos_test::EventCapture events;
    auto t = LogosTestContext("storage_module");
    auto* impl = createInitializedImpl(t);

    LOGOS_ASSERT_TRUE(impl->stop().success);
    LOGOS_ASSERT_TRUE(events.has("storageStop"));

    impl->destroy();
    delete impl;
}
