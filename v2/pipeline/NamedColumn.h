#pragma once

#include <string_view>

namespace db {
class Column;
}

namespace db::v2 {

class PipelineBlock;

class NamedColumn {
public:
    friend PipelineBlock;

    Column* getColumn() const { return _col; }

    std::string_view getName() const { return _name; }

    void setName(std::string_view name);

    static NamedColumn* create(PipelineBlock* block,
                               Column* col,
                               std::string_view name);

private:
    PipelineBlock* _parent {nullptr};
    Column* _col {nullptr};
    std::string_view _name;

    NamedColumn(PipelineBlock* block,
                Column* col,
                std::string_view name);
    ~NamedColumn();
};

}
