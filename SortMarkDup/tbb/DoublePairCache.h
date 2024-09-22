/**
 * A class used to store the DoublePair temporarily
 * author: lhh
 */

#ifndef DOUBLE_PAIR_CACHE_H
#define DOUBLE_PAIR_CACHE_H

#include <vector>
#include "pair.h"

#define INITIAL_LENGTH 0x1000

class DoublePairCache{
private:
    std::vector<void *> cache;
    uint32_t offset;

public:
    DoublePairCache();
    ~DoublePairCache();

    DoublePair * getSpace();
};


#endif