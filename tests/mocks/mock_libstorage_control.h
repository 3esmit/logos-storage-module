#pragma once

// Test-only controls for one mocked chunk request. They allow a unit test to
// keep a V2 operation active long enough to verify cancellation and terminal
// event correlation without relying on timing.
void mockStorageSetNextDownloadChunkPayload(const char* payload);
void mockStorageHoldNextDownloadChunk();
bool mockStorageWaitForHeldDownloadChunk(int timeoutMs);
void mockStorageCompleteHeldDownloadChunk(int result, const char* payload,
                                          const char* message);

void mockStorageHoldNextDownloadInit();
bool mockStorageWaitForHeldDownloadInit(int timeoutMs);
void mockStorageCompleteHeldDownloadInit(int result, const char* message);
bool mockStorageHeldDownloadInitSessionOpen();
void mockStorageResetDownloadCancelObservation();
bool mockStorageWaitForDownloadCancel(int timeoutMs);
void mockStorageHoldNextDownloadCancel();
bool mockStorageWaitForHeldDownloadCancel(int timeoutMs);
void mockStorageCompleteHeldDownloadCancel(int result, const char* message);

void mockStorageHoldNextDownloadStream();
bool mockStorageWaitForHeldDownloadStream(int timeoutMs);
void mockStorageCompleteHeldDownloadStream(int result, const char* message);
