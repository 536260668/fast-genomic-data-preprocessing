#include <cstddef>
#include <cstdint>
namespace sorted_doubles_rmi {
bool load(char const* dataPath);
void cleanup();
const size_t RMI_SIZE = 3221225488;
const uint64_t BUILD_TIME_NS = 660278631857;
const char NAME[] = "sorted_doubles_rmi";
uint64_t lookup(double key, size_t* err);
}
