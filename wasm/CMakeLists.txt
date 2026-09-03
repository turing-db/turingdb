cmake_minimum_required(VERSION 3.15)

project(turingdb-wasm)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Werror -Wunused-function -Wunused-variable)

set(TURINGDB_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_library(turing_db_wasm_s STATIC
    Column.cpp
    ColumnContainer.cpp)

# Header-only use of the wire protocol definitions (enums, ColumnWireHeader).
# Do not link turing_db_proto_common_s here: it is a native-build library.
target_include_directories(turing_db_wasm_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC ${TURINGDB_ROOT}/net/decoders
    PUBLIC ${TURINGDB_ROOT}/net/turing_proto_common)

add_executable(turing_db_wasm_smoke SmokeTest.cpp)
target_link_libraries(turing_db_wasm_smoke PRIVATE turing_db_wasm_s)

if (EMSCRIPTEN)
    # Decoder memory needs scale with result size: let the wasm heap grow past
    # its initial allocation (old JS TypedArray views detach on growth).
    target_link_options(turing_db_wasm_smoke PRIVATE -sALLOW_MEMORY_GROWTH=1)

    # The JS-facing decoder module: the shared decode core (net/decoders) compiled
    # against the wasm sink family, exposed to JS through embind. The listed native
    # sources are the portable subset the decode path needs; storage/list is used
    # header-only (tags and the tag dispatcher).
    add_executable(turing_wasm_decoder
        DecoderModule.cpp
        WasmSink.cpp
        Column.cpp
        ColumnContainer.cpp
        ${TURINGDB_ROOT}/net/decoders/DecodedColumnSchema.cpp
        ${TURINGDB_ROOT}/net/turing_proto_common/ChunkedBuffer.cpp
        ${TURINGDB_ROOT}/net/turing_proto_common/ProtocolException.cpp
        ${TURINGDB_ROOT}/net/turing_proto_common/TuringProtoHeaders.cpp
        ${TURINGDB_ROOT}/net/turing_proto_common/TuringProtoInBuf.cpp
        ${TURINGDB_ROOT}/net/turing_proto_common/TuringProtoOutBuf.cpp
        ${TURINGDB_ROOT}/common/BioAssert.cpp
        ${TURINGDB_ROOT}/common/FatalException.cpp
        ${TURINGDB_ROOT}/common/TuringException.cpp
        ${TURINGDB_ROOT}/storage/EntityList.cpp)

    target_include_directories(turing_wasm_decoder PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${TURINGDB_ROOT}/net/decoders
        ${TURINGDB_ROOT}/net/turing_proto_common
        ${TURINGDB_ROOT}/storage
        ${TURINGDB_ROOT}/common)

    target_include_directories(turing_wasm_decoder SYSTEM PRIVATE
        ${TURINGDB_ROOT}/external/expected/include
        ${TURINGDB_ROOT}/external/spdlog/include)

    # bioassert formats through spdlog's bundled fmt; header-only keeps spdlog itself
    # out of the build.
    target_compile_definitions(turing_wasm_decoder PRIVATE FMT_HEADER_ONLY=1)

    # The decode paths throw TuringException; wasm needs exceptions opted in. The
    # exception handling helpers give JS Module.getExceptionMessage to read a thrown
    # exception's type and message.
    target_compile_options(turing_wasm_decoder PRIVATE -fwasm-exceptions)
    target_link_options(turing_wasm_decoder PRIVATE
        -fwasm-exceptions
        -sEXPORT_EXCEPTION_HANDLING_HELPERS=1
        -lembind
        -sALLOW_MEMORY_GROWTH=1
        -sWASM_BIGINT=1
        -sMODULARIZE=1
        -sEXPORT_NAME=createTuringDecoderModule)
endif()
