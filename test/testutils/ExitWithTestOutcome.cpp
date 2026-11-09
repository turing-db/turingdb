#include "ExitWithTestOutcome.h"

#include <stdlib.h>
#include <unistd.h>

#include <iostream>

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

namespace turing::test {

void exitWithTestOutcome(void (*scenario)(), unsigned timeoutSeconds) {
    alarm(timeoutSeconds);

    testing::TestPartResultArray results;

    {
        const testing::ScopedFakeTestPartResultReporter reporter(&results);
        scenario();
    }

    bool failed = false;
    for (int i = 0; i < results.size(); i++) {
        const testing::TestPartResult& result = results.GetTestPartResult(i);

        if (result.failed()) {
            std::cerr << result << std::endl;
            failed = true;
        }
    }

    exit(failed ? EXIT_FAILURE : EXIT_SUCCESS);
}

}
