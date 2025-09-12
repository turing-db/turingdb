#pragma once

#include <vector>

#include "NodePattern.h"
#include "EdgePattern.h"

namespace db::v2 {

class CypherAST;

class PatternElement {
public:
    friend CypherAST;
    using EntityPatterns = std::vector<EntityPattern*>;

    // Provides an edge-node view of a pattern element chain
    class ChainIterator {
    public:
        explicit ChainIterator(EntityPatterns::const_iterator it)
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
        EntityPatterns::const_iterator _it;
    };

    class ChainView {
    public:
        ChainView(EntityPatterns::const_iterator begin,
                  EntityPatterns::const_iterator end)
            : _begin(begin),
              _end(end)
        {
        }

        explicit ChainView(const EntityPatterns& entities)
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
        EntityPatterns::const_iterator _begin;
        EntityPatterns::const_iterator _end;
    };

    static PatternElement* create(CypherAST* ast);

    // Add a node or edge at the beginning of the chain
    void addRootEntity(EntityPattern* entity) {
        _entities.insert(_entities.begin(), entity);
    }

    // Add a node or edge at the end of the chain
    void addEntity(EntityPattern* entity) {
        _entities.emplace_back(entity);
    }

    const EntityPatterns& getEntities() const {
        return _entities;
    }

    EntityPattern* getRootEntity() const {
        return _entities.front();
    }

    ChainView getElementChain() const {
        if (_entities.size() <= 1) {
            return ChainView(_entities.end(), _entities.end());
        }

        return ChainView(_entities.begin() + 1, _entities.end());
    }

private:
    EntityPatterns _entities;

    PatternElement();
    ~PatternElement();
};

}
