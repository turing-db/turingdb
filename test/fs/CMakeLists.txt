add_turing_test(fs_bytebufferiterator_test ByteBufferIteratorTest.cpp)
add_turing_test(fs_filewriter_test FileWriterTest.cpp)
add_turing_test(fs_filepagewriter_test FilePageWriterTest.cpp)
add_turing_test(fs_filepagereader_test FilePageReaderTest.cpp)
add_turing_test(fs_fileresult_test FileResultTest.cpp)

target_link_libraries(fs_bytebufferiterator_test
    PRIVATE turing_fs_s)

target_link_libraries(fs_filewriter_test
    PRIVATE turing_fs_s)

target_link_libraries(fs_filepagereader_test
    PRIVATE turing_fs_s)

target_link_libraries(fs_filepagewriter_test
    PRIVATE turing_fs_s)

target_link_libraries(fs_fileresult_test
    PRIVATE turing_fs_s)
