#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A column a hop carries is checked to come from the rows the hop reads. Each run goes
// behind padding of another size, which moves the map that check walks over the heap.
class CarriedColumnRowSourceTest : public WriteQueryTest {
protected:
    static constexpr size_t RUN_COUNT = 256;
    static constexpr size_t PADDING_STEP = 24;
};

TEST_F(CarriedColumnRowSourceTest, carriesACoalesceOfADeepSumAcrossAHop) {
    // 46 terms make the coalesce the 48th column the check records, where its map grows
    static constexpr size_t TERM_COUNT = 46;

    std::string sum = "n.age";
    for (size_t term = 1; term < TERM_COUNT; term++) {
        sum += " + n.age";
    }

    const std::string query = "MATCH (n:Person) WITH n, coalesce(" + sum + ", 0) AS x "
                              "MATCH (n)-->(m) RETURN n.name, x, m.name";

    const Rows expected = {{"Remy", "1472", "Adam"},
                           {"Remy", "1472", "Ghosts"},
                           {"Remy", "1472", "Computers"},
                           {"Remy", "1472", "Eighties"},
                           {"Adam", "1472", "Remy"},
                           {"Adam", "1472", "Bio"},
                           {"Adam", "1472", "Cooking"},
                           {"Maxime", "0", "Bio"},
                           {"Maxime", "0", "Padel"},
                           {"Luc", "0", "Animals"},
                           {"Luc", "0", "Computers"},
                           {"Martina", "0", "Cooking"},
                           {"Suhas", "0", "Gym"},
                           {"Suhas", "0", "JiuJitsu"},
                           {"Cyrus", "0", "Gym"},
                           {"Cyrus", "0", "Travel"},
                           {"Doruk", "0", "Gym"}};

    std::vector<std::unique_ptr<char[]>> padding;
    for (size_t run = 0; run < RUN_COUNT; run++) {
        padding.push_back(std::make_unique<char[]>(run * PADDING_STEP));

        expectRows(query, expected);
        ASSERT_FALSE(HasFailure()) << "run " << run;
    }
}
