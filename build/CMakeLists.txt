

# Get build timestamp
execute_process(COMMAND date +%s
    OUTPUT_VARIABLE BUILD_TIMESTAMP)

message(STATUS "Last commit hash: ${HEAD_COMMIT_HASH}")

# ========== Time Bomb settings =================
set(TIME_BOMB_ACTIVE TRUE)
set(TIME_BOMB_DAYS 10)

# ===== Generate build info file
configure_file(BuildInfo.h.in BuildInfo.h)
set(BUILD_INFO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
