/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "string_chunk.h"

#include "rmutil/alloc.h"

Chunk_t *String_NewChunk(size_t size) {
    StringChunk *newChunk = (StringChunk *)malloc(sizeof(StringChunk));
    newChunk->num_samples = 0;
    newChunk->size = size;
    newChunk->samples = (StringSample *)malloc(size);

    return newChunk;
}

void String_FreeChunk(Chunk_t *chunk) {
    free(((StringChunk *)chunk)->samples);
    free(chunk);
}

/**
 * TODO: describe me
 * @param chunk
 * @return
 */
Chunk_t *String_SplitChunk(Chunk_t *chunk) {
    StringChunk *curChunk = (StringChunk *)chunk;
    size_t split = curChunk->num_samples / 2;
    size_t curNumSamples = curChunk->num_samples - split;

    // create chunk and copy samples
    StringChunk *newChunk = String_NewChunk(split * SAMPLE_SIZE);
    for (size_t i = 0; i < split; ++i) {
        StringSample *sample = &curChunk->samples[curNumSamples + i];
        String_AddSample(newChunk, sample);
    }

    // update current chunk
    curChunk->num_samples = curNumSamples;
    curChunk->size = curNumSamples * SAMPLE_SIZE;
    curChunk->samples = realloc(curChunk->samples, curChunk->size);

    return newChunk;
}

static int IsChunkFull(StringChunk *chunk) {
    return chunk->num_samples == chunk->size / SAMPLE_SIZE;
}

u_int64_t String_NumOfSample(Chunk_t *chunk) {
    return ((StringChunk *)chunk)->num_samples;
}

static StringSample *ChunkGetSample(StringChunk *chunk, int index) {
    return &chunk->samples[index];
}

timestamp_t String_GetLastTimestamp(Chunk_t *chunk) {
    if (((StringChunk *)chunk)->num_samples == 0) {
        return -1;
    }
    return ChunkGetSample(chunk, ((StringChunk *)chunk)->num_samples - 1)->timestamp;
}

timestamp_t String_GetFirstTimestamp(Chunk_t *chunk) {
    if (((StringChunk *)chunk)->num_samples == 0) {
        return -1;
    }
    return ChunkGetSample(chunk, 0)->timestamp;
}

ChunkResult String_AddSample(Chunk_t *chunk, StringSample *sample) {
    StringChunk *regChunk = (StringChunk *)chunk;
    if (IsChunkFull(regChunk)) {
        return CR_END;
    }

    if (String_NumOfSample(regChunk) == 0) {
        // initialize base_timestamp
        regChunk->base_timestamp = sample->timestamp;
    }
    // give sample value to
    regChunk->samples[regChunk->num_samples] = *sample;
    regChunk->samples[regChunk->num_samples].value = (sample->value);
    regChunk->num_samples++;

    return CR_OK;
}

/**
 * TODO: describe me
 * @param chunk
 * @param idx
 * @param sample
 */
static void upsertChunk(StringChunk *chunk, size_t idx, StringSample *sample) {
    if (chunk->num_samples == chunk->size / SAMPLE_SIZE) {
        chunk->size += sizeof(StringSample);
        chunk->samples = realloc(chunk->samples, chunk->size);
    }
    if (idx < chunk->num_samples) { // sample is not last
        memmove(&chunk->samples[idx + 1],
                &chunk->samples[idx],
                (chunk->num_samples - idx) * sizeof(StringSample));
    }
    chunk->samples[idx] = *sample;
    chunk->num_samples++;
}

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult String_UpsertSample(UpsertCtxStr *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    *size = 0;
    StringChunk *regChunk = (StringChunk *)uCtx->inChunk;
    timestamp_t ts = uCtx->sample.timestamp;
    short numSamples = regChunk->num_samples;
    // find sample location
    size_t i = 0;
    StringSample *sample = NULL;
    for (; i < numSamples; ++i) {
        sample = ChunkGetSample(regChunk, i);
        if (ts <= sample->timestamp) {
            break;
        }
    }
    // update value in case timestamp exists
    if (sample != NULL && ts == sample->timestamp) {
        ChunkResult cr = handleDuplicateSampleStr(duplicatePolicy, *sample, &uCtx->sample);
        if (cr != CR_OK) {
            return CR_ERR;
        }
        regChunk->samples[i].value = uCtx->sample.value;
        return CR_OK;
    }

    if (i == 0) {
        regChunk->base_timestamp = ts;
    }

    upsertChunk(regChunk, i, &uCtx->sample);
    *size = 1;
    return CR_OK;
}

ChunkIter_t *String_NewChunkIterator(Chunk_t *chunk,
                                           int options,
                                     ChunkIterFuncs *retChunkIterClass) {
    StringChunkIterator *iter = (StringChunkIterator *)calloc(1, sizeof(StringChunkIterator));
    iter->chunk = chunk;
    iter->options = options;
    if (options & CHUNK_ITER_OP_REVERSE) { // iterate from last to first
        iter->currentIndex = iter->chunk->num_samples - 1;
    } else { // iterate from first to last
        iter->currentIndex = 0;
    }

    if (retChunkIterClass != NULL) {
        *retChunkIterClass = *GetChunkIteratorClass(CHUNK_STRING);
    }

    return (ChunkIter_t *)iter;
}

ChunkResult String_ChunkIteratorGetNext(ChunkIter_t *iterator, StringSample *sample) {
    StringChunkIterator *iter = (StringChunkIterator *)iterator;
    if (iter->currentIndex < iter->chunk->num_samples) {
        *sample = *ChunkGetSample(iter->chunk, iter->currentIndex);
        iter->currentIndex++;
        return CR_OK;
    } else {
        return CR_END;
    }
}

ChunkResult String_ChunkIteratorGetPrev(ChunkIter_t *iterator, StringSample *sample) {
    StringChunkIterator *iter = (StringChunkIterator *)iterator;
    if (iter->currentIndex >= 0) {
        *sample = *ChunkGetSample(iter->chunk, iter->currentIndex);
        iter->currentIndex--;
        return CR_OK;
    } else {
        return CR_END;
    }
}

void String_FreeChunkIterator(ChunkIter_t *iterator) {
    StringChunkIterator *iter = (StringChunkIterator *)iterator;
    if (iter->options & CHUNK_ITER_OP_FREE_CHUNK) {
        String_FreeChunk(iter->chunk);
    }
    free(iter);
}

size_t String_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    StringChunk *stringchunk = chunk;
    size_t size = stringchunk->size;
    size += includeStruct ? sizeof(*stringchunk) : 0;
    return size;
}

void String_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    StringChunk *stringchunk = chunk;

    RedisModule_SaveUnsigned(io, stringchunk->base_timestamp);
    RedisModule_SaveUnsigned(io, stringchunk->num_samples);
    RedisModule_SaveUnsigned(io, stringchunk->size);

    RedisModule_SaveStringBuffer(io, (char *)stringchunk->samples, stringchunk->size);
}

void String_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    StringChunk *stringchunk = (StringChunk *)malloc(sizeof(*stringchunk));

    stringchunk->base_timestamp = RedisModule_LoadUnsigned(io);
    stringchunk->num_samples = RedisModule_LoadUnsigned(io);
    stringchunk->size = RedisModule_LoadUnsigned(io);

    stringchunk->samples = (StringSample *)RedisModule_LoadStringBuffer(io, NULL);
    *chunk = (Chunk_t *)stringchunk;
}
