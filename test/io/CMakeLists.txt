add_turing_test(test_io_path PathTest.cpp)
add_turing_test(test_io_bytebufferiterator ByteBufferIteratorTest.cpp)
add_turing_test(test_io_filewriter FileWriterTest.cpp)
add_turing_test(test_io_filepagewriter FilePageWriterTest.cpp)
add_turing_test(test_io_filepagereader FilePageReaderTest.cpp)
add_turing_test(test_io_fileresult FileResultTest.cpp)
add_turing_test(test_io_s3 S3Test.cpp)
add_turing_test(test_io_filecache FileCacheTest.cpp)

target_link_libraries(test_io_path
    PRIVATE turing_db_io_fs_s)

target_link_libraries(test_io_bytebufferiterator
    PRIVATE turing_db_io_fs_s)

target_link_libraries(test_io_filewriter
    PRIVATE turing_db_io_fs_s)

target_link_libraries(test_io_filepagereader
    PRIVATE turing_db_io_fs_s)

target_link_libraries(test_io_filepagewriter
    PRIVATE turing_db_io_fs_s)

target_link_libraries(test_io_fileresult
    PRIVATE turing_db_io_fs_s)

target_link_libraries(test_io_s3
    PRIVATE turing_db_io_s3_s)

target_link_libraries(test_io_filecache
    PRIVATE turing_db_io_filecache_s)
