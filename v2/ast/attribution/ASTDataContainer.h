#pragma once

#include <vector>

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

    template <typename T, typename... Args>
    T& newAnalysisData(EvaluatedType type, Args&&... args) {
        auto& data = _data.emplace_back();
        return data.emplace<T>(std::forward<Args>(args)...);
    }

private:
    std::vector<AnalysisData> _data;
};

}
