cmake_minimum_required(VERSION 3.16)
project(SortMarkDup)

set(CMAKE_CXX_STANDARD 17)
# compile flag      # pay attention, different mode is set here
set(CMAKE_CXX_FLAGS "-g -Wall -Wextra -fno-omit-frame-pointer -pthread")
set(CMAKE_CXX_FLAGS_DEBUG "-fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# linker flag
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -W")
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
set(CMAKE_EXE_LINKER_FLAGS_RELEASE "${CMAKE_EXE_LINKER_FLAGS}")

# counterpart of gcc -I
# using lhh's htslib
include_directories("${PROJECT_SOURCE_DIR}/htslib/htslib")

file (GLOB TBB_FILE "./tbb/*.cpp")

add_executable(tbb-sormadup main.cpp "${TBB_FILE}")

# Includes
target_include_directories(tbb-sormadup PRIVATE "${PROJECT_SOURCE_DIR}/../oneTBB/binary/include")

# counterpart of gcc -L
target_link_directories (tbb-sormadup PRIVATE "${PROJECT_SOURCE_DIR}/../oneTBB/binary/lib/intel64/gcc4.8/")
target_link_libraries (tbb-sormadup tbb)


# using lhh's htslib
target_link_libraries (tbb-sormadup "${PROJECT_SOURCE_DIR}/htslib/libhts.so")
target_link_libraries (tbb-sormadup tcmalloc)
target_link_libraries (tbb-sormadup "${PROJECT_SOURCE_DIR}/lz4/lib/liblz4.so")


# # find_package(PkgConfig REQUIRED)
# # pkg_check_modules (JEMALLOC jemalloc)
# # pkg_search_module(JEMALLOC REQUIRED jemalloc)
# # include_directories(${JEMALLOC_INCLUDE_DIRS})
