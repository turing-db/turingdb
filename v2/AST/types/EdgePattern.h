#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "types/EntityPattern.h"

namespace db::v2 {

class MapLiteral;

struct EdgePatternData;

class EdgePattern : public EntityPattern {
public:
    using EdgeTypeVector = std::vector<std::string_view>;

    EdgePattern() = default;
    ~EdgePattern() override = default;

    EdgePattern(const EdgePattern&) = default;
    EdgePattern(EdgePattern&&) = default;
    EdgePattern& operator=(const EdgePattern&) = default;
    EdgePattern& operator=(EdgePattern&&) = default;

    static std::unique_ptr<EdgePattern> create() {
        return std::make_unique<EdgePattern>();
    }

    bool hasTypes() const {
        return _types.has_value();
    }

    bool hasDecl() const {
        return _decl != nullptr;
    }

    bool hasData() const {
        return _data != nullptr;
    }

    const EdgeTypeVector& types() const {
        return _types.value();
    }

    EdgeTypeVector& types() {
        return _types.value();
    }

    const VarDecl& decl() const {
        return *_decl;
    }

    const EdgePatternData& data() const {
        return *_data;
    }

    void setTypes(std::optional<EdgeTypeVector>&& types) {
        _types = std::move(types);
    }

    void setDecl(const VarDecl* decl) {
        _decl = decl;
    }

    void setData(EdgePatternData* data) {
        _data = data;
    }

private:
    std::optional<EdgeTypeVector> _types;
    const VarDecl* _decl {nullptr};
    EdgePatternData* _data {nullptr};
};

}
