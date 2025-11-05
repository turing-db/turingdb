#pragma once

#define GOTOCASE(ClassName, CaseCode)  \
GotoCase_##ClassName: \
{                     \
    CaseCode          \
}                     \

#define GOTOPTR(ClassName) &&GotoCase_##ClassName
