set(turings3client_sources TuringS3Client.cpp
                           S3ClientResult.cpp
                           AwsManager.cpp
                           MockS3Client.cpp)

add_library(turing_db_io_s3_s STATIC ${turings3client_sources})

target_link_libraries(turing_db_io_s3_s PUBLIC
        ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(turing_db_io_s3_s PUBLIC
        turing_common_s)

target_include_directories(turing_db_io_s3_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

