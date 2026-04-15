#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

TuringTestEnv::TuringTestEnv()
    : _db(&_config)
{
}

// Some suites need planner flags baked into the DB before init(), so the
// QueryConfig overload forwards them at construction time.
TuringTestEnv::TuringTestEnv(const QueryConfig& defaultQueryConfig)
    : _db(&_config, defaultQueryConfig)
{
}

TuringTestEnv::~TuringTestEnv() = default;

std::unique_ptr<TuringTestEnv> TuringTestEnv::create(const fs::Path& turingDir) {
    auto env = std::make_unique<TuringTestEnv>();

    // Unit tests need to spawn concurrent turingdb instances
    // so we have to disable system events (because the handler is
    // static -> shared by all instances)
    env->_config.useSystemEvents(false);
    env->_config.setSyncedOnDisk(false);
    env->_config.setTuringDirectory(turingDir);

    env->_db.init();

    return env;
}

std::unique_ptr<TuringTestEnv> TuringTestEnv::create(const fs::Path& turingDir,
                                                     const QueryConfig& defaultQueryConfig) {
    auto env = std::make_unique<TuringTestEnv>(defaultQueryConfig);

    // Unit tests need to spawn concurrent turingdb instances
    // so we have to disable system events (because the handler is
    // static -> shared by all instances)
    env->_config.useSystemEvents(false);
    env->_config.setSyncedOnDisk(false);
    env->_config.setTuringDirectory(turingDir);

    env->_db.init();

    return env;
}

std::unique_ptr<TuringTestEnv> TuringTestEnv::createSyncedOnDisk(const fs::Path& turingDir) {
    auto env = std::make_unique<TuringTestEnv>();

    // Unit tests need to spawn concurrent turingdb instances
    // so we have to disable system events (because the handler is 
    // static -> shared by all instances)
    env->_config.useSystemEvents(false);
    env->_config.setSyncedOnDisk(true);
    env->_config.setTuringDirectory(turingDir);
    env->_db.init();

    return env;
}
