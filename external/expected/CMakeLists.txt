project(expected VERSION 0.8.0)

add_library(expected INTERFACE)
add_library(expected::expected ALIAS expected)
target_include_directories(expected INTERFACE include)
