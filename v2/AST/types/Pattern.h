#pragma once

#include <optional>

#include "types/PatternElement.h"
#include "types/WhereClause.h"

namespace db::v2 {

class Pattern {
public:
    Pattern() = default;
    ~Pattern() = default;

    Pattern(const Pattern&) = delete;
    Pattern(Pattern&&) = delete;
    Pattern& operator=(const Pattern&) = delete;
    Pattern& operator=(Pattern&&) = delete;

    static std::unique_ptr<Pattern> create() {
        return std::make_unique<Pattern>();
    }

    void setWhere(const WhereClause& where) {
        _where = where;
    }

    bool hasWhere() const {
        return _where.has_value();
    }

    const WhereClause& getWhere() const {
        return _where.value();
    }

    void addElem(PatternElement* part) {
        _elems.push_back(part);
    }

    const std::vector<PatternElement*>& elements() const {
        return _elems;
    }

private:
    std::vector<PatternElement*> _elems;
    std::optional<WhereClause> _where;
};

}
