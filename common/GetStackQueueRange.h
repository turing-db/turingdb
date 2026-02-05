#pragma once

// Utility class to get a range on std::stack or std::queue
// because std::stack and std::queue don't provide iterator access
// without modification
template <typename StackOrQueue>
class GetStackQueueRange {
public:
    GetStackQueueRange() = delete;
    ~GetStackQueueRange() = delete;

    static const auto& getRange(const StackOrQueue& ds) {
        return HackedStructure().getUnderlyingContainer(ds);
    }

private:
    struct HackedStructure : private StackOrQueue {
        const auto& getUnderlyingContainer(const StackOrQueue& ds) {
            return ((const HackedStructure&)ds).c;
        }
    };
};
