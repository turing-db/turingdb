add_turing_test(fs_bytebufferiterator_test ByteBufferIteratorTest.cpp)
add_turing_test(fs_filewriter_test FileWriterTest.cpp)

target_link_libraries(fs_bytebufferiterator_test
    PRIVATE turing_fs_s)

target_link_libraries(fs_filewriter_test
    PRIVATE turing_fs_s)
