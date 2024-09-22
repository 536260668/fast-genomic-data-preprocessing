/**
 * The implementation of SinglePairCache class
 * author: lhh
 */
#include <iostream>
#include "SinglePairCache.h"

SinglePairCache::SinglePairCache() : offset(0)
{
    cache.emplace_back(malloc(sizeof(SinglePair) * INITIAL_LENGTH));
}

SinglePairCache::~SinglePairCache()
{
    for(auto & singlePairArray : cache)
    {
        free(singlePairArray);
    }
}

SinglePair * SinglePairCache::getSpace()
{
    if(offset >= INITIAL_LENGTH)
    {
        cache.emplace_back(malloc(sizeof(SinglePair) * INITIAL_LENGTH));
        offset = 0;
    }
    SinglePair * result = (SinglePair*)cache.back() + offset;
    offset++;
    return result;
}