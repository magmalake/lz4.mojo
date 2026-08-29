cmake_minimum_required(VERSION 3.16)
project(lz4mojo C)

# No CMake config package ships with conda-forge's lz4-c; find it by name.
find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
find_library(LZ4_LIBRARY NAMES lz4)

if(NOT LZ4_INCLUDE_DIR OR NOT LZ4_LIBRARY)
    message(FATAL_ERROR "liblz4 not found (expected the conda-forge lz4-c package)")
endif()

add_library(lz4mojo SHARED lz4_wrapper.c)
target_include_directories(lz4mojo PRIVATE ${LZ4_INCLUDE_DIR})
target_link_libraries(lz4mojo PRIVATE ${LZ4_LIBRARY})
# Natural per-platform extension: liblz4mojo.dylib on macOS, .so on Linux.
# _find_lib() in the Mojo package tries both, in that order, at runtime.
set_target_properties(lz4mojo PROPERTIES PREFIX "lib")

install(TARGETS lz4mojo LIBRARY DESTINATION lib)
