/**
 * A class used to store the SinglePair temporarily
 * author: lhh
 */

#ifndef SINGLE_PAIR_CACHE_H
#define SINGLE_PAIR_CACHE_H

#include <vector>
#include "pair.h"

#define INITIAL_LENGTH 0x1000

class SinglePairCache{
private:
    std::vector<void *> cache;
    uint32_t offset;

public:
    SinglePairCache();
    ~SinglePairCache();

    SinglePair * getSpace();
};


#endif