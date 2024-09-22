/**
 * The implementation of DoublePairCache class
 * author: lhh
 */
#include <iostream>
#include "DoublePairCache.h"

DoublePairCache::DoublePairCache() : offset(0)
{
    cache.emplace_back(malloc(sizeof(DoublePair) * INITIAL_LENGTH));
}

DoublePairCache::~DoublePairCache()
{
    for(auto & DoublePairArray : cache)
    {
        free(DoublePairArray);
    }
}

DoublePair * DoublePairCache::getSpace()
{
    if(offset >= INITIAL_LENGTH)
    {
        cache.emplace_back(malloc(sizeof(DoublePair) * INITIAL_LENGTH));
        offset = 0;
    }
    DoublePair * result = (DoublePair*)cache.back() + offset;
    offset++;
    return result;
}