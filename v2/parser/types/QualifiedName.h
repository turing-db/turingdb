#pragma once

#include <vector>
#include <string_view>

namespace db {

class QualifiedName {
public:
    QualifiedName() = default;
    ~QualifiedName() = default;

    QualifiedName(const QualifiedName&) = default;
    QualifiedName(QualifiedName&&) = default;
    QualifiedName& operator=(const QualifiedName&) = default;
    QualifiedName& operator=(QualifiedName&&) = default;

    void addName(const std::string_view& name) { _names.emplace_back(name); }

    const std::vector<std::string_view>& getNames() const { return _names; }

private:
    std::vector<std::string_view> _names;
};

}
