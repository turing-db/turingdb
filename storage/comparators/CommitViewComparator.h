#pragma once

namespace db {

class CommitView;

class CommitViewComparator {
public:
    [[nodiscard]] static bool same(const CommitView& a, const CommitView& b);
};

}
