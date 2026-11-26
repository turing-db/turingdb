#pragma once

namespace turing::test {

// Starts the interpreter the test process runs Python in, once. It lives until the process
// exits: CPython does not support a second initialization after Py_FinalizeEx, and
// GTEST_REPEAT runs every suite again.
void initializeEmbeddedPython();

}
