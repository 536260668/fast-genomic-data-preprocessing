# This is hacky :(

file(GLOB_RECURSE bustub_sources ${PROJECT_SOURCE_DIR}/bustub/*/*.cpp ${PROJECT_SOURCE_DIR}/bustub/*/*/*.cpp)
add_library(bustub_shared SHARED ${bustub_sources})

######################################################################################################################
# THIRD-PARTY SOURCES
######################################################################################################################
