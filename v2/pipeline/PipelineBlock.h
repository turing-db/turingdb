#pragma once

#include <vector>

namespace db::v2 {

class NamedColumn;

class PipelineBlock {
public:
    friend NamedColumn;

    PipelineBlock();
    ~PipelineBlock();

private:
    std::vector<NamedColumn*> _cols;

    void addColumn(NamedColumn* col);
};

}
