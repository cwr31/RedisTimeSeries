/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef STRING_CHUNK_H
#define STRING_CHUNK_H

#include "consts.h"
#include "generic_chunk.h"
#include <sys/types.h>

typedef struct StringChunk {
    timestamp_t base_timestamp;
    StringSample *samples;
    unsigned int num_samples;
    size_t size;
} StringChunk;

typedef struct StringChunkIterator {
    StringChunk *chunk;
    int currentIndex;
    timestamp_t lastTimestamp;
    int lastValue;
    int options;
} StringChunkIterator;

Chunk_t *String_NewChunk(size_t sampleCount);
void String_FreeChunk(Chunk_t *chunk);

/**
 * TODO: describe me
 * @param chunk
 * @return
 */
Chunk_t *String_SplitChunk(Chunk_t *chunk);
size_t String_GetChunkSize(Chunk_t *chunk, bool includeStruct);

/**
 * TODO: describe me
 * 0 for failure, 1 for success
 * @param chunk
 * @param sample
 * @return
 */
ChunkResult String_AddSample(Chunk_t *chunk, StringSample *sample);

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult String_UpsertSample(UpsertCtxStr *uCtx, int *size, DuplicatePolicy duplicatePolicy);

u_int64_t String_NumOfSample(Chunk_t *chunk);
timestamp_t String_GetLastTimestamp(Chunk_t *chunk);
timestamp_t String_GetFirstTimestamp(Chunk_t *chunk);

ChunkIter_t *String_NewChunkIterator(Chunk_t *chunk, int options, ChunkIterFuncs* retChunkIterClass);
ChunkResult String_ChunkIteratorGetNext(ChunkIter_t *iterator, StringSample *sample);
ChunkResult String_ChunkIteratorGetPrev(ChunkIter_t *iterator, StringSample *sample);
void String_FreeChunkIterator(ChunkIter_t *iter);

// RDB
void String_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io);
void String_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io);

#endif
