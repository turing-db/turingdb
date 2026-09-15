cmake_minimum_required(VERSION 3.15)

# Standalone when build_npm.sh configures this directory with emcmake; a subdirectory of
# the native build otherwise, where the parent has already called project().
if (NOT DEFINED PROJECT_NAME)
    project(turingdb-wasm)
endif()

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

    # The decoder ships in the npm package, so it is built where the package is
    # assembled - beside TuringClient.mjs, which imports it as a sibling. This mirrors
    # python/turingdb/_binary, where the nanobind modules are built into the package.
    # CMAKE_CURRENT_SOURCE_DIR, not CMAKE_SOURCE_DIR: this directory is its own project,
    # configured standalone by build_npm.sh, so CMAKE_SOURCE_DIR is wasm/ itself.
    #
    # .mjs, not .js: EXPORT_ES6 emits real ES module syntax, which is what lets the
    # browser load the decoder natively and what lets emscripten locate the wasm through
    # import.meta.url. A CommonJS decoder only ever works under Node and bundlers.
    set_target_properties(turing_wasm_decoder PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/npm
        SUFFIX ".mjs")

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
        -sEXPORT_ES6=1
        -sEXPORT_NAME=createTuringDecoderModule)
else()
    # DecoderModule.cpp is the only source that needs emscripten (embind), so everything
    # else still compiles with the host compiler and SmokeTest runs as an ordinary binary.
    # That gives the shared column code coverage in ctest, which the decoder cannot have.
    target_link_libraries(turing_db_wasm_smoke PRIVATE turing_db_proto_common_s turing_common_s)

    add_test(NAME test_wasm_columns COMMAND turing_db_wasm_smoke)
endif()
