#pragma once

#include <vector>

#include "attribution/ASTNodeID.h"
#include "attribution/EvaluatedType.h"
#include "attribution/AnalysisData.h"

namespace db {

class ASTDataContainer {
public:
    ASTDataContainer();
    ~ASTDataContainer();

    ASTDataContainer(const ASTDataContainer&) = delete;
    ASTDataContainer& operator=(const ASTDataContainer&) = delete;
    ASTDataContainer(ASTDataContainer&&) = delete;
    ASTDataContainer& operator=(ASTDataContainer&&) = delete;

    AnalysisData& newAnalysisData(EvaluatedType type) {
        auto id = _data.size();
        auto data = std::make_unique<AnalysisData>(id, type);
        auto* ptr = data.get();
        _data.push_back(std::move(data));

        return *ptr;
    }

    const AnalysisData& get(ASTNodeID id) const {
        return *_data[id.value()];
    }

    AnalysisData& get(ASTNodeID id) {
        return *_data[id.value()];
    }

private:
    std::vector<std::unique_ptr<AnalysisData>> _data;
};

}
