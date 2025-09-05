#pragma once

#include <variant>
#include <memory>

namespace db::v2 {

class VarDecl;

struct NodePatternData;
struct EdgePatternData;

class AnalysisData {
public:
    template <typename T>
    using UniquePtr = std::unique_ptr<T, void (*)(T*)>;

    using Variant = std::variant<std::monostate,
                                 UniquePtr<NodePatternData>,
                                 UniquePtr<EdgePatternData>>;
    AnalysisData() = default;
    ~AnalysisData() = default;

    AnalysisData(const AnalysisData&) = delete;
    AnalysisData(AnalysisData&&) = default;
    AnalysisData& operator=(const AnalysisData&) = delete;
    AnalysisData& operator=(AnalysisData&&) = default;

    template <typename T>
    bool is() const {
        return std::holds_alternative<std::unique_ptr<T, void (*)(T*)>>(_data);
    }

    template <typename T>
    T& as() {
        return *std::get<UniquePtr<T>>(_data);
    }

    template <typename T>
    const T& as() const {
        return *std::get<UniquePtr<T>>(_data);
    }

    template <typename T, typename... Args>
    T& emplace(Args&&... args) {
        return *_data.emplace<UniquePtr<T>>(T::create(std::forward<Args>(args)...));
    }

private:
    Variant _data;
};


}
