#pragma once

namespace db {
class DB;

struct DBComparator {
public:
    static bool same(const DB* db1, const DB* db2);

protected:
    template <typename T>
    static bool same(const T* obj1, const T* obj2);

    /* @brief Compares std::map<StringRef, U*> objects
     *
     * This is a deep comparison since DBComparator::same<U> is called
     * For example, compareMappedContainers<PropertyTypes> compares
     * the property types owners, value types and names. Note that the keys
     * (StringRef) are not compared since DBComparator::same<U> already compares
     * the names stored in U
     * */
    template <typename T>
    static bool deepCompareMaps(const T* c1, const T* c2);

    /* @brief Compares std::vector<U*> objects
     *
     * This is a deep comparison since DBComparator::same<U> is called
     * */
    template <typename T>
    static bool deepCompareVectors(const T* c1, const T* c2);
};

} // namespace db
