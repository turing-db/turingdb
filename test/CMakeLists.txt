add_subdirectory(googletest EXCLUDE_FROM_ALL SYSTEM)
include(GoogleTest)

# Fix for LLVM clang: googletest's -Wconversion triggers -Wcharacter-conversion warnings
# that become errors with our -Werror flag
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    target_compile_options(gtest PRIVATE -Wno-character-conversion)
    target_compile_options(gtest_main PRIVATE -Wno-character-conversion)
endif()

add_subdirectory(testutils)
add_subdirectory(testenv)
add_subdirectory(common)
add_subdirectory(io)
add_subdirectory(storage)
add_subdirectory(import)
add_subdirectory(query)
add_subdirectory(system)
add_subdirectory(jobs)
add_subdirectory(serialisation)
