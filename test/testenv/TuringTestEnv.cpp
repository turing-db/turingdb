#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

TuringTestEnv::TuringTestEnv() = default;

TuringTestEnv::~TuringTestEnv() = default;

std::unique_ptr<TuringTestEnv> TuringTestEnv::create(const fs::Path& turingDir) {
    auto env = std::make_unique<TuringTestEnv>();

    env->_config.setSyncedOnDisk(false);
    env->_config.setTuringDirectory(turingDir);
    env->_db.init();

    return env;
}

std::unique_ptr<TuringTestEnv> TuringTestEnv::createSyncedOnDisk(const fs::Path& turingDir) {
    auto env = std::make_unique<TuringTestEnv>();

    env->_config.setSyncedOnDisk(true);
    env->_config.setTuringDirectory(turingDir);
    env->_db.init();

    return env;
}
