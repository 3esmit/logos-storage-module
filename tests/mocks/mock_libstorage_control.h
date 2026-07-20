#pragma once

// Test-only controls for one mocked chunk request. They allow a unit test to
// keep a V2 operation active long enough to verify cancellation and terminal
// event correlation without relying on timing.
void mockStorageSetNextDownloadChunkPayload(const char* payload);
void mockStorageHoldNextDownloadChunk();
bool mockStorageWaitForHeldDownloadChunk(int timeoutMs);
void mockStorageCompleteHeldDownloadChunk(int result, const char* payload,
                                          const char* message);
