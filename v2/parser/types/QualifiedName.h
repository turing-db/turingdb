#pragma once

#include <vector>
#include <string>

namespace db {

class QualifiedName {
public:
    QualifiedName() = default;
    ~QualifiedName() = default;

    QualifiedName(const QualifiedName&) = default;
    QualifiedName(QualifiedName&&) = default;
    QualifiedName& operator=(const QualifiedName&) = default;
    QualifiedName& operator=(QualifiedName&&) = default;

    void addName(std::string&& name) { _names.emplace_back(std::move(name)); }

    const std::vector<std::string>& getNames() const { return _names; }

private:
    std::vector<std::string> _names;
};

}
