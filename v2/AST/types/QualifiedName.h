#pragma once

#include <vector>
#include <string_view>

namespace db::v2 {

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

    std::vector<std::string_view>::const_iterator begin() const { return _names.begin(); }
    std::vector<std::string_view>::const_iterator end() const { return _names.end(); }

    size_t size() const { return _names.size(); }

    std::string_view get(size_t index) const { return _names[index]; }

private:
    std::vector<std::string_view> _names;
};

}
