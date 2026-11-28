#include <gtest/gtest.h>

#include <Python.h>

#include "EmbeddedPython.h"

using namespace turing::test;

TEST(EmbeddedPythonTest, objectsAreAllocatedWithMalloc) {
    initializeEmbeddedPython();

    PyMemAllocatorEx rawAllocator;
    PyMem_GetAllocator(PYMEM_DOMAIN_RAW, &rawAllocator);

    PyMemAllocatorEx memoryAllocator;
    PyMem_GetAllocator(PYMEM_DOMAIN_MEM, &memoryAllocator);

    PyMemAllocatorEx objectAllocator;
    PyMem_GetAllocator(PYMEM_DOMAIN_OBJ, &objectAllocator);

    EXPECT_EQ(memoryAllocator.malloc, rawAllocator.malloc);
    EXPECT_EQ(objectAllocator.malloc, rawAllocator.malloc);
}
