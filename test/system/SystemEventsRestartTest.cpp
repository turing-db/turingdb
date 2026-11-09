#include <gtest/gtest.h>

#include "TuringConfig.h"
#include "TuringDB.h"
#include "TuringException.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// A database with system events on owns the process's one signal and socket handler, as
// the embedded Python module's does
class SystemEventsRestartTest : public TuringTest {
protected:
    void configure(TuringConfig& config, const char* directory) {
        config.setSyncedOnDisk(false);
        config.setTuringDirectory(fs::Path {_outDir + "/" + directory});
    }
};

TEST_F(SystemEventsRestartTest, aSecondDatabaseStartsAfterTheFirstIsDestroyed) {
    TuringConfig config;
    configure(config, "turing");

    {
        TuringDB first(&config);
        ASSERT_NO_THROW(first.init());
    }

    TuringDB second(&config);
    EXPECT_NO_THROW(second.init());
}

TEST_F(SystemEventsRestartTest, aDatabaseThatCannotStartLeavesTheRunningHandler) {
    TuringConfig runningConfig;
    configure(runningConfig, "running");

    TuringDB running(&runningConfig);
    ASSERT_NO_THROW(running.init());

    {
        TuringConfig secondConfig;
        configure(secondConfig, "second");

        TuringDB second(&secondConfig);
        EXPECT_THROW(second.init(), TuringException);
    }

    EXPECT_TRUE(runningConfig.getSocketPath().exists());
}
