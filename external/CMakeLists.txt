add_subdirectory(argparse)
add_subdirectory(expected)
add_subdirectory(linenoise)
add_subdirectory(tabulate)
add_subdirectory(range-v3)
add_subdirectory(spdlog)
add_subdirectory(termcolor)
add_subdirectory(nanobind)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/datasets/cyber-security-ad.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/healthcare-analytics.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/movies.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/network-management.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/openstreetmap.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/poledb.jsonl
              ${CMAKE_CURRENT_SOURCE_DIR}/datasets/recommendations.jsonl
        DESTINATION datasets)
