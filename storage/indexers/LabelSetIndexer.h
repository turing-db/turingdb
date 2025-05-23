#pragma once

#include <map>

#include "metadata/LabelSetHandle.h"

namespace db {

template <typename T>
class LabelSetIndexer {
public:
    class LabelSetComparator {
    public:
        bool operator()(const LabelSetHandle& a, const LabelSetHandle& b) const {
            return a.getID() < b.getID();
        }
    };

    using InternalContainer = std::map<LabelSetHandle, T, LabelSetComparator>;

    class MatchIterator {
    public:
        MatchIterator() = default;
        MatchIterator(const InternalContainer* container,
                      const LabelSetHandle& compare)
            : _container(container),
              _compare(compare),
              _it(container->begin())
        {
            for (; _it != _container->end(); _it++) {
                if (_it->first.hasAtLeastLabels(compare)) {
                    return;
                }
            }
        }

        const LabelSetHandle& getKey() const {
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

                if (_it->first.hasAtLeastLabels(_compare)) {
                    return;
                }
            }
        }

    private:
        const InternalContainer* _container {nullptr};
        LabelSetHandle _compare;
        InternalContainer::const_iterator _it {};
    };

    explicit LabelSetIndexer()
    {
    }

    InternalContainer::const_iterator find(const LabelSetHandle& labelset) const {
        return _container.find(labelset);
    }

    const T& at(const LabelSetHandle& labelset) const {
        return _container.at(labelset);
    }

    bool contains(const LabelSetHandle& labelset) const {
        return _container.contains(labelset);
    }

    size_t size() const {
        return _container.size();
    }

    T& operator[](const LabelSetHandle& labelset) {
        return _container[labelset];
    }

    const InternalContainer& getContainer() const {
        return _container;
    }

    MatchIterator matchIterate(const LabelSetHandle& matchingLabelSet) const {
        return MatchIterator(&_container, matchingLabelSet);
    }

    InternalContainer::const_iterator begin() const { return _container.begin(); }
    InternalContainer::const_iterator end() const { return _container.end(); }
    InternalContainer::iterator begin() { return _container.begin(); }
    InternalContainer::iterator end() { return _container.end(); }

private:
    InternalContainer _container;
};

}
