set(fs_sources File.cpp
               FileResult.cpp
               FileRegion.cpp
               FilePageReader.cpp
               FilePageWriter.cpp
               Path.cpp)

add_library(turing_fs_s
    STATIC  ${fs_sources})

target_link_libraries(turing_fs_s
    PRIVATE common_s)

target_include_directories(turing_fs_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})
