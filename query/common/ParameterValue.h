#pragma once

#include <stdint.h>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/Point.h"
#include "metadata/PropertyType.h"

namespace db {

class ParameterValue {
public:
    using List = std::vector<ParameterValue>;
    using Map = std::map<std::string, ParameterValue>;
    using Embedding = std::vector<float>;
    using Variant = std::variant<std::monostate, int64_t, double, bool, std::string, List, Map, Duration, DateTime, Embedding, Point>;

    ParameterValue();
    ParameterValue(const ParameterValue& other);
    ParameterValue(ParameterValue&& other) noexcept;
    ~ParameterValue();

    ParameterValue& operator=(const ParameterValue& other);
    ParameterValue& operator=(ParameterValue&& other) noexcept;

    bool isNull() const;
    ValueType getType() const;
    std::string_view getTypeName() const;

    int64_t getInt64() const;
    double getDouble() const;
    bool getBool() const;
    const std::string& getString() const;
    const List& getList() const;
    const Map& getMap() const;
    Duration getDuration() const;
    DateTime getDateTime() const;
    std::span<const float> getEmbedding() const;
    Point getPoint() const;

    void setNull();
    void setInt64(int64_t value);
    void setDouble(double value);
    void setBool(bool value);
    void setString(std::string_view value);
    std::string& setString();
    List& setList();
    Map& setMap();
    void setDuration(Duration value);
    void setDateTime(DateTime value);
    void setEmbedding(std::span<const float> value);
    Embedding& setEmbedding();
    void setPoint(Point value);

private:
    Variant _value;

    bool holdsContainer() const;
    void copyFrom(const ParameterValue& other);
    void detachContainer(std::vector<Variant>& worklist);

    static void detachChildren(Variant& container, std::vector<Variant>& worklist);
};

}
