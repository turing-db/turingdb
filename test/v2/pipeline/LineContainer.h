#pragma once

#include <ostream>
#include <unordered_map>
#include <spdlog/fmt/bundled/format.h>

template <typename... ValueTypes>
class LineContainer {
public:
    struct Line {
        std::tuple<ValueTypes...> _values;

        struct Hash {
        private:
            template <std::size_t... Is>
            std::size_t hashImpl(const Line& line, std::index_sequence<Is...>) const {
                std::size_t hash = 0;
                (combineHash(hash, std::get<Is>(line._values)), ...);
                return hash;
            }

        public:
            std::size_t operator()(const Line& line) const {
                return hashImpl(line, std::index_sequence_for<ValueTypes...> {});
            }
        };

        struct Predicate {
            bool operator()(const Line& lhs, const Line& rhs) const {
                return lhs._values == rhs._values;
            }
        };
    };

    void add(std::tuple<ValueTypes...> values) {
        _lineMap[Line {values}]++;
    }

    void clear() {
        _lineMap.clear();
    }

    std::size_t getCount(std::tuple<ValueTypes...> values) const {
        const Line line {values};
        const auto it = _lineMap.find(line);
        if (it == _lineMap.end()) {
            return 0;
        }
        return it->second;
    }

    bool equals(const LineContainer<ValueTypes...>& other) const {
        if (_lineMap.size() != other._lineMap.size()) {
            return false;
        }

        for (const auto& [line, count] : _lineMap) {
            const auto it = other._lineMap.find(line);
            if (it == other._lineMap.end()) {
                return false;
            }

            if (it->second != count) {
                return false;
            }
        }

        return true;
    }

    void print(std::ostream& os) const {
        for (const auto& [line, count] : _lineMap) {
            os << "[";
            printTuple(os, line._values);
            os << "]: " << count << std::endl;
        }
    }

private:
    std::unordered_map<Line, std::size_t, typename Line::Hash, typename Line::Predicate> _lineMap;

    template <typename Tuple, std::size_t... Is>
    static void printTupleImpl(std::ostream& os, const Tuple& tup, std::index_sequence<Is...>) {
        (fmt::print(os, Is == 0 ? "{:>3}" : ", {:>3}", std::get<Is>(tup)), ...);
    }

    template <typename... Ts>
    static void printTuple(std::ostream& os, const std::tuple<Ts...>& tup) {
        printTupleImpl(os, tup, std::index_sequence_for<Ts...> {});
    }

    template <typename T>
    static void combineHash(std::size_t& seed, const T& value) {
        seed ^= std::hash<T> {}(value);
    }
};
