#include "ParseOnStack.h"

#include <pthread.h>
#include <exception>

#include <gtest/gtest.h>

#include "CypherAST.h"
#include "CypherParser.h"

using namespace db;

namespace {

struct ParseRun {
    const std::string* _query {nullptr};
    std::string* _error {nullptr};
};

void* parseOnThread(void* argument) {
    ParseRun* const run = static_cast<ParseRun*>(argument);

    CypherAST ast(nullptr, *run->_query);
    CypherParser parser(&ast);

    try {
        parser.parse(*run->_query);
    } catch (const std::exception& exception) {
        *run->_error = exception.what();
    }

    return nullptr;
}

}

void turing::test::buildNestedQuery(const Nesting& nesting, size_t levels, std::string& query) {
    query = nesting._prefix;
    for (size_t level = 0; level < levels; level++) {
        query += nesting._open;
    }

    query += nesting._core;

    for (size_t level = 0; level < levels; level++) {
        query += nesting._close;
    }
}

void turing::test::parseOnStack(const std::string& query, size_t stackBytes, std::string& error) {
    ParseRun run {&query, &error};

    pthread_attr_t attributes;
    ASSERT_EQ(pthread_attr_init(&attributes), 0);
    ASSERT_EQ(pthread_attr_setstacksize(&attributes, stackBytes), 0);

    pthread_t thread;
    ASSERT_EQ(pthread_create(&thread, &attributes, parseOnThread, &run), 0);
    ASSERT_EQ(pthread_join(thread, nullptr), 0);
    pthread_attr_destroy(&attributes);
}
