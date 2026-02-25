#pragma once

namespace db {

class Commit;

class CommitComparator {
public:
    [[nodiscard]] static bool same(const Commit* a, const Commit* b);
};

}
