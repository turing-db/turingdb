
add_library(linenoise_s STATIC linenoise.c)
target_include_directories(linenoise_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

