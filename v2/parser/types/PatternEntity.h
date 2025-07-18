#pragma once

#include <string_view>
#include <memory>

namespace db {

class PatternEntity {
public:
    PatternEntity() = default;
    virtual ~PatternEntity() = default;

    PatternEntity(const PatternEntity&) = default;
    PatternEntity(PatternEntity&&) = default;
    PatternEntity& operator=(const PatternEntity&) = default;
    PatternEntity& operator=(PatternEntity&&) = default;

    static std::unique_ptr<PatternEntity> create() {
        return std::make_unique<PatternEntity>();
    }

private:
    std::string_view _name;
};

}
