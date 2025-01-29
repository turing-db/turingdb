#pragma once

#include "JumpTableMacros.h"

#define EXECUTE_STEP()                                   \
    step.execute();                                      \
    pipeStep++;                                          \
    goto *activateTbl[(uint64_t)pipeStep->getOpcode()];  \

#define RETURN_FROM_STEP()                            \
    pipeStep--;                                       \
    goto *returnTbl[(uint64_t)pipeStep->getOpcode()]; \

#define ACTIVATE_STEP(StepClass) GOTOCASE(Activate##StepClass, {    \
    StepClass& step = pipeStep->get<StepClass>();                   \
    step.reset();                                                   \
    EXECUTE_STEP()                                                  \
})                                                                  \

#define ACTIVATE_PTR(StepClass) GOTOPTR(Activate##StepClass)

#define ACTIVATE_END() GOTOCASE(ActivateEnd, {     \
    RETURN_FROM_STEP()                             \
})                                                 \

#define ACTIVATE_END_PTR() GOTOPTR(ActivateEnd)

#define RETURN_STEP(StepClass) GOTOCASE(Return##StepClass, {    \
    StepClass& step = pipeStep->get<StepClass>();               \
    if (step.isFinished()) {                                    \
        RETURN_FROM_STEP()                                      \
    } else {                                                    \
        EXECUTE_STEP()                                          \
    }                                                           \
})                                                              \

#define RETURN_PTR(StepClass) GOTOPTR(Return##StepClass)
