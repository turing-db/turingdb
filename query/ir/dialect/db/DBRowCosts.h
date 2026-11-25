#pragma once

namespace mlir::db {

constexpr double STORAGE_READ_ROW_COST = 10.0;
constexpr double STRING_TEST_ROW_COST = 3.0;
constexpr double MEMBERSHIP_TEST_ROW_COST = 5.0;
constexpr double CONTAINER_BUILD_ROW_COST = 15.0;
constexpr double VECTOR_FUNCTION_ROW_COST = 40.0;

}
