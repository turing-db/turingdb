#pragma once

#include "EntityID.h"
#include "labels/LabelSetMap.h"

namespace db {

template <typename T>
class LabelSetIndexer {
public:
    using InternalContainer = std::unordered_map<LabelSetID, T>;

    class MatchIterator {
    public:
        MatchIterator() = default;
        MatchIterator(const LabelSetMap* map,
                      const InternalContainer* container,
                      const LabelSet* compare)
            : _map(map),
              _container(container),
              _compare(compare),
              _it(container->begin())
        {
            for (; _it != _container->end(); _it++) {
                const LabelSet& current = _map->getValue(_it->first);

                if (current.hasAtLeastLabels(*compare)) {
                    return;
                }
            }
        }

        LabelSetID getID() const {
            return _it->first;
        }

        const T& getValue() const {
            return _it->second;
        }

        bool isValid() const {
            return _it != _container->end();
        }

        MatchIterator& operator++() {
            next();
            return *this;
        }

        MatchIterator operator++(int) {
            auto it = *this;
            next();
            return it;
        }

        void next() {
            while (true) {
                _it++;

                if (!isValid()) {
                    return;
                }

                const LabelSet& current = _map->getValue(_it->first);
                if (current.hasAtLeastLabels(*_compare)) {
                    return;
                }
            }
        }

    private:
        const LabelSetMap* _map {nullptr};
        const InternalContainer* _container {nullptr};
        const LabelSet* _compare {nullptr};
        InternalContainer::const_iterator _it {};
    };

    explicit LabelSetIndexer(const LabelSetMap* labelsetMap)
        : _labelsetMap(labelsetMap)
    {
    }

    InternalContainer::const_iterator find(LabelSetID labelsetID) const {
        return _container.find(labelsetID);
    }

    const T& at(LabelSetID id) const {
        return _container.at(id);
    }

    bool contains(LabelSetID id) const {
        return _container.contains(id);
    }

    size_t size() const {
        return _container.size();
    }

    T& operator[](LabelSetID id) {
        return _container[id];
    }

    const InternalContainer& getContainer() const {
        return _container;
    }

    MatchIterator matchIterate(const LabelSet* matchingLabelSet) const {
        return MatchIterator(_labelsetMap, &_container, matchingLabelSet);
    }

    InternalContainer::const_iterator begin() const { return _container.begin(); }
    InternalContainer::const_iterator end() const { return _container.end(); }

private:
    InternalContainer _container;
    const LabelSetMap* _labelsetMap = nullptr;
};

}
