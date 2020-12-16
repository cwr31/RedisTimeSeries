/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef GENERIC__CHUNK_H
#define GENERIC__CHUNK_H

#include <sys/types.h>
#include <rmutil/strings.h>
#include <stdlib.h>         // malloc
#include <stdio.h>          // printf
#include <string.h>         // memcpy, memmove
#include "consts.h"

struct RedisModuleIO;

typedef struct Sample {
    timestamp_t timestamp;
    double value;
} Sample;

typedef struct StringSample {
    timestamp_t timestamp;
    char *value;
} StringSample;

typedef void Chunk_t;
typedef void ChunkIter_t;

#define CHUNK_ITER_OP_NONE 0
#define CHUNK_ITER_OP_REVERSE 1
// This is supported *only* by uncompressed chunk, this is used when we reverse read a compressed chunk by
// uncompressing it into a *un*compressed chunk and returning a reverse iterator on that "temporary" uncompressed chunk.
#define CHUNK_ITER_OP_FREE_CHUNK 1 << 2

typedef enum {
    CHUNK_REGULAR,
    CHUNK_COMPRESSED,
    CHUNK_STRING
} CHUNK_TYPES_T;

typedef struct UpsertCtx {
    Sample sample;
    Chunk_t *inChunk;       // original chunk  
} UpsertCtx;

typedef struct UpsertCtxStr {
    StringSample sample;
    Chunk_t *inChunk;       // original chunk
} UpsertCtxStr;

typedef struct ChunkIterFuncs {
    void(*Free)(ChunkIter_t *iter);
    ChunkResult(*GetNext)(ChunkIter_t *iter, Sample *sample);
    ChunkResult(*GetNextStr)(ChunkIter_t *iter, StringSample *sample);
    ChunkResult(*GetPrev)(ChunkIter_t *iter, Sample *sample);
    ChunkResult(*GetPrevStr)(ChunkIter_t *iter, StringSample *sample);
} ChunkIterFuncs;


typedef struct ChunkFuncs {
    Chunk_t *(*NewChunk)(size_t sampleCount);
    void(*FreeChunk)(Chunk_t *chunk);
    Chunk_t *(*SplitChunk)(Chunk_t *chunk);

    ChunkResult(*AddSample)(Chunk_t *chunk, Sample *sample);
    ChunkResult(*AddSampleStr)(Chunk_t *chunk, StringSample *sample);
    ChunkResult(*UpsertSample)(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);
    ChunkResult(*UpsertSampleStr)(UpsertCtxStr *uCtx, int *size, DuplicatePolicy duplicatePolicy);

    ChunkIter_t *(*NewChunkIterator)(Chunk_t *chunk, int options, ChunkIterFuncs* retChunkIterClass);

    size_t(*GetChunkSize)(Chunk_t *chunk, bool includeStruct);
    u_int64_t(*GetNumOfSample)(Chunk_t *chunk);
    u_int64_t(*GetLastTimestamp)(Chunk_t *chunk);
    u_int64_t(*GetFirstTimestamp)(Chunk_t *chunk);

    void (*SaveToRDB)(Chunk_t *chunk, struct RedisModuleIO *io);
    void (*LoadFromRDB)(Chunk_t **chunk, struct RedisModuleIO *io);
} ChunkFuncs;

ChunkResult handleDuplicateSample(DuplicatePolicy policy, Sample oldSample, Sample *newSample);
ChunkResult handleDuplicateSampleStr(DuplicatePolicy policy, StringSample oldSample, StringSample *newSample);
const char * DuplicatePolicyToString(DuplicatePolicy policy);
int RMStringLenDuplicationPolicyToEnum(RedisModuleString *aggTypeStr);
DuplicatePolicy DuplicatePolicyFromString(const char *input, size_t len);

ChunkFuncs *GetChunkClass(CHUNK_TYPES_T chunkClass);
ChunkIterFuncs *GetChunkIteratorClass(CHUNK_TYPES_T chunkType);

#endif //GENERIC__CHUNK_H
