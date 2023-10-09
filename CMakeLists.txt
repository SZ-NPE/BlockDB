
cmake_minimum_required(VERSION 3.12)
project(BlockDB)


##--------------------- Version file ---------------------------------------##

# Specified the language standard
set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)

##--------------------- Compile Options ------------------------------------##
# Configure compile options
add_compile_options(-Wall) # -Wextra -pedantic -Wshadow -Wstrict-prototypes --all-warnings -Wconversion # -Werror
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pipe -std=c99")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pipe -std=c++17")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native -mtune=native")

# Set build type
# set(CMAKE_BUILD_TYPE Debug)  # Use `cmake -DCMAKE_BUILD_TYPE=Debug` more better
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Compile options for Debug variant
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

# Compile options for Release variant
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O2")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")

set(THREADS_PREFER_PTHREAD_FLAG ON)
add_definitions(-DGFLAGS=1)

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
    add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    add_definitions(-DOS_MACOSX)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
        add_definitions(-DIOS_CROSS_COMPILE -DROCKSDB_LITE)
        # no debug info for IOS, that will make our library big
        add_definitions(-DNDEBUG)
    endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
    add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
    add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
    add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
    add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
    add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
    add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
    add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
    if(MINGW)
        add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
    endif()
endif()

set(CMAKE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")


include_directories(
        "${PROJECT_BINARY_DIR}/include"
        "."
)
#include_directories(include)

aux_source_directory(db db)
aux_source_directory(helpers/memenv helper)
aux_source_directory(port port)
aux_source_directory(table table)
aux_source_directory(util util)

aux_source_directory(test-blockdb/src demo)

# set(src_files "lib.c kv.c wh.c blkio.c sst.c xdb.c")
add_library(blockdb STATIC ${db} ${helper} ${port} ${table} ${util})
#target_include_directories(blockdb PRIVATE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/db)
target_link_libraries(blockdb PUBLIC pthread aio)
target_include_directories(blockdb
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)



add_executable(simple_test test-blockdb/src/simple.cc test-blockdb/src/test_blockdb.cc)
target_link_libraries(simple_test blockdb)