#pragma once

#include <vector>

#include "EntityPattern.h"
#include "NodePattern.h"
#include "EdgePattern.h"

namespace db {

class NodePattern;
class EdgePattern;

class PatternElement {
public:
    class ChainIterator {
    public:
        explicit ChainIterator(std::vector<EntityPattern*>::const_iterator it)
            : _it(it)
        {
        }

        std::pair<EdgePattern*, NodePattern*> operator*() const {
            return std::make_pair(static_cast<EdgePattern*>(*_it),
                                  static_cast<NodePattern*>(*(_it + 1)));
        }

        ChainIterator& operator++() {
            _it += 2;
            return *this;
        }

        bool operator!=(const ChainIterator& other) const {
            return _it != other._it;
        }

    private:
        std::vector<EntityPattern*>::const_iterator _it;
    };

    class ChainView {
    public:
        ChainView(std::vector<EntityPattern*>::const_iterator begin,
                  std::vector<EntityPattern*>::const_iterator end)
            : _begin(begin),
              _end(end)
        {
        }

        explicit ChainView(const std::vector<EntityPattern*>& entities)
            : _begin(entities.begin()),
              _end(entities.end())
        {
        }

        ChainIterator begin() const {
            return ChainIterator(_begin);
        }

        ChainIterator end() const {
            return ChainIterator(_end);
        }

    private:
        std::vector<EntityPattern*>::const_iterator _begin;
        std::vector<EntityPattern*>::const_iterator _end;
    };

    PatternElement() = default;
    ~PatternElement() = default;

    PatternElement(const PatternElement&) = default;
    PatternElement(PatternElement&&) = default;
    PatternElement& operator=(const PatternElement&) = default;
    PatternElement& operator=(PatternElement&&) = default;

    static std::unique_ptr<PatternElement> create() {
        return std::make_unique<PatternElement>();
    }

    void addRootNode(NodePattern* node) {
        _entities.insert(_entities.begin(), (EntityPattern*)node);
    }

    void addRootEdge(EdgePattern* edge) {
        _entities.insert(_entities.begin(), (EntityPattern*)edge);
    }

    void addNode(NodePattern* node) {
        _entities.emplace_back(node);
    }

    void addEdge(EdgePattern* edge) {
        _entities.emplace_back(edge);
    }

    const std::vector<EntityPattern*>& getEntities() const {
        return _entities;
    }

    NodePattern* getRootNode() const {
        return static_cast<NodePattern*>(_entities.front());
    }

    ChainView getElementChain() const {
        if (_entities.size() <= 1) {
            return ChainView(_entities.end(), _entities.end());
        }

        return ChainView(_entities.begin() + 1, _entities.end());
    }

private:
    std::vector<EntityPattern*> _entities;
};

}
