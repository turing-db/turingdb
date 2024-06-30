#include "utils.h"

#include <vector>
#include <cstdint>
#include <range/v3/view/iota.hpp>
#include <range/v3/range/conversion.hpp>

void getTestProgram(std::vector<db::Instruction>& instructions) {
    instructions = {
        //db::Instruction::getDBCount(0),     // Stores db count in reg0
        //db::Instruction::createDatabase(1), // Create database with name "DB #n" with n == value stored in reg1
        //db::Instruction::getDBCount(2),     // Stores db count in reg2
        //db::Instruction::listDatabases(3),  // populates a std::vector<std::string_view> object with db names
        //db::Instruction::exit(),  // populates a std::vector<std::string_view> object with db names
    };
}

uint64_t getCount() {
    return 400'001;
}
