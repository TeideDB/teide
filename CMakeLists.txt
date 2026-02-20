cmake_minimum_required(VERSION 3.15)
project(teide LANGUAGES C)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(TEIDE_PORTABLE "Build portable binary (no -march=native)" OFF)

# Collect all source files under src/
file(GLOB_RECURSE TEIDE_SOURCES CONFIGURE_DEPENDS "src/**/*.c")

# Static library
add_library(teide_static STATIC ${TEIDE_SOURCES})
set_target_properties(teide_static PROPERTIES OUTPUT_NAME teide)
target_include_directories(teide_static PUBLIC include PRIVATE src)

# Shared library
add_library(teide SHARED ${TEIDE_SOURCES})
target_include_directories(teide PUBLIC include PRIVATE src)

# Platform-specific compiler flags
if(MSVC)
    set(TEIDE_WARNINGS /W4 /WX)
    set(TEIDE_DEBUG_FLAGS /Od /Zi /fsanitize=address)
    set(TEIDE_RELEASE_FLAGS /O2 /DNDEBUG /arch:AVX2)
else()
    set(TEIDE_WARNINGS -Wall -Wextra -Wpedantic -Werror)
    set(TEIDE_DEBUG_FLAGS -O0 -g -fsanitize=address,undefined)
    if(TEIDE_PORTABLE)
        set(TEIDE_RELEASE_FLAGS -O3 -mtune=generic -DNDEBUG)
    else()
        set(TEIDE_RELEASE_FLAGS -O3 -march=native -DNDEBUG)
    endif()
endif()

# Apply flags to both library targets
foreach(tgt teide_static teide)
    target_compile_options(${tgt} PRIVATE ${TEIDE_WARNINGS})
    target_compile_options(${tgt} PRIVATE
        $<$<CONFIG:Debug>:${TEIDE_DEBUG_FLAGS}>
        $<$<CONFIG:Release>:${TEIDE_RELEASE_FLAGS}>
    )
endforeach()

# Link sanitizer runtime for debug builds (GCC/Clang)
if(NOT MSVC)
    foreach(tgt teide_static teide)
        target_link_options(${tgt} PRIVATE
            $<$<CONFIG:Debug>:-fsanitize=address,undefined>
        )
    endforeach()
endif()

# Platform-specific link libraries
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(teide_static PUBLIC m pthread)
    target_link_libraries(teide PUBLIC m pthread)
endif()

# Test executable
enable_testing()
file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "test/*.c")
if(TEST_SOURCES)
    add_executable(test_teide ${TEST_SOURCES})
    target_link_libraries(test_teide PRIVATE teide_static)
    target_include_directories(test_teide PRIVATE include src test)
    target_compile_options(test_teide PRIVATE ${TEIDE_WARNINGS})
    target_compile_options(test_teide PRIVATE
        $<$<CONFIG:Debug>:${TEIDE_DEBUG_FLAGS}>
        $<$<CONFIG:Release>:${TEIDE_RELEASE_FLAGS}>
    )
    if(NOT MSVC)
        target_link_options(test_teide PRIVATE
            $<$<CONFIG:Debug>:-fsanitize=address,undefined>
        )
    endif()
    add_test(NAME test_teide COMMAND test_teide)
endif()
