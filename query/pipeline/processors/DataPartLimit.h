#pragma once

namespace db {

class ChangeAccessor;
class TuringConfig;

/**
 * @brief Throws @ref PipelineException if the graph's main head data parts plus the data
 * parts already committed by the in-progress change reach @ref
 * TuringConfig::getMaxDataParts, instructing the user to run MERGE_DATAPARTS. Enforced
 * before COMMIT and CHANGE SUBMIT materialise more parts, so that a graph cannot
 * accumulate an unbounded number of data parts without being compacted.
 */
void throwIfTooManyDataParts(const ChangeAccessor& access, const TuringConfig* config);

}
