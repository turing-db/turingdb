add_subdirectory(argparse)
add_subdirectory(expected)
add_subdirectory(linenoise)
add_subdirectory(tabulate)
add_subdirectory(range-v3)
add_subdirectory(spdlog)
add_subdirectory(termcolor)
add_subdirectory(nanobind)

# Google Benchmark, for microbenchmarks under test/ (EXCLUDE_FROM_ALL: only built when linked).
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
add_subdirectory(benchmark EXCLUDE_FROM_ALL SYSTEM)
# The project compiles with -Werror; silence warnings in this third-party target.
target_compile_options(benchmark PRIVATE -w)
target_compile_options(benchmark_main PRIVATE -w)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/datasets/cyber-security-ad.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/healthcare-analytics.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/movies.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/network-management.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/openstreetmap.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/poledb.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/recommendations.jsonl
        DESTINATION datasets)
