#pragma once

#include <stddef.h>
#include <optional>

#include "NLStepBuffers.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "metadata/PropertyType.h"

namespace db {

// Type of handles per column type that writes the input rows selected
// by the indices column into output
using NLGatherFunction = void (*)(const Column* input,
                                  const ColumnVector<size_t>* indices,
                                  Column* output);

// A cross product pairs every outer row with every inner row, so the pair at
// position p is outer row p/M with inner row p%M (M being the inner row count).
// An input column holds only its N or M values, so filling a column of pairs
// repeats them: that repetition is the broadcast. With outer [a0,a1] (N=2) and
// inner [b0,b1,b2] (M=3) the whole product reads:
//   outer -> [a0,a0,a0, a1,a1,a1]   each outer row repeated M times (block-repeat)
//   inner -> [b0,b1,b2, b0,b1,b2]   the inner chunk repeated N times (tile)
// so row k across all columns is one pair.
//
// A step fills the `rowCount` pairs starting at `position`, which is how the
// product is cut into chunks: a slice may start and end mid-block or mid-tile, and
// every column of the step slices at the same position, so they stay row-aligned.
// `factor` is M for both directions - block-repeat divides the position by it,
// tile takes the position modulo it.
using NLBroadcastFunction = void (*)(const Column* input,
                                     size_t factor,
                                     size_t position,
                                     size_t rowCount,
                                     Column* output);

// Type of handle that appends the indices of the rows an nl.filter keeps into the
// indices column. One per mask nullability: a plain mask keeps every true row, a
// nullable mask keeps only present-and-true rows (a null drops).
using NLMaskSurvivorFunction = void (*)(const Column* mask,
                                        ColumnVector<size_t>* indices);

// Wraps a "carried" column from previous operations
class NLCarriedColumn {
public:
    NLCarriedColumn(const Column* input,
                    Column* output,
                    NLGatherFunction gather)
        : _input(input),
        _output(output),
        _gather(gather)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLGatherFunction getGatherFunc() const { return _gather; }

private:
    // Carried column pre-filtering
    const Column* _input {nullptr};

    // Output column after filtering
    Column* _output {nullptr};

    // Gather function to be used to process indices
    NLGatherFunction _gather {nullptr};
};

// The rows one cell of an unwound column contributes: a list its element count, a null
// none, and any other value the single row it is. One per source column shape, selected
// during translation.
using NLUnwindElementCountFunction = size_t (*)(const Column* source, size_t row);

// Fill the element chunk of one nl.unwind step from a column whose cells hold more than
// the element: output row i is the element at positions[i] of the cell at rows[i].
using NLUnwindElementEmitFunction = void (*)(const Column* source,
                                             const ColumnVector<size_t>* rows,
                                             const ColumnVector<size_t>* positions,
                                             Column* output);

// One column used as operand of nl.cross_product: its input chunk, the output
// chunk to fill, and the broadcast that fills one from the other.
class NLCrossColumn {
public:
    NLCrossColumn(const Column* input,
                  Column* output,
                  NLBroadcastFunction broadcast)
        : _input(input),
        _output(output),
        _broadcast(broadcast)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLBroadcastFunction getBroadcast() const { return _broadcast; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    NLBroadcastFunction _broadcast {nullptr};
};

// Copies rows [inputOffset, inputOffset + rowCount) of an input chunk into a
// fresh, front-aligned output chunk. The skip suffix copy: nl.skip_truncate sets
// inputOffset = skipThisStep and rowCount = emitThisStep, lifting the surviving
// suffix to the front of the output. The block-repeat/tile broadcasts copy from
// row zero and so cannot express the offset; this family does. Cleared and
// resized to rowCount, the copy is a plain range copy (lowered to memcpy).
using NLCopyFunction = void (*)(const Column* input,
                                size_t inputOffset,
                                size_t rowCount,
                                Column* output);

// Read the cell one element column holds at @param row as the value the list buffer
// stores for it, tagged with the type it is. One per column kind, selected during
// translation the way the case write families are. The memory is what a column owning its
// characters copies them into, as the concatenation kernel copies its result.
using NLListItemReadFunction = ListBuffer<>::ListItemVariant (*)(const Column* input,
                                                                 size_t row,
                                                                 NLStepBuffers* stepBuffers);

// Read the bound one column of an nl.range holds at @param row, or nothing where the row
// has no bound. One per integer column kind, selected during translation the way the list
// item reads are.
using NLRangeBoundReadFunction = std::optional<types::Int64::Primitive> (*)(const Column* input,
                                                                           size_t row);

struct NLRangeBound {
    const Column* _column {nullptr};
    NLRangeBoundReadFunction _read {nullptr};
};

// Type of handle that fills a column with a run of null rows - for an ID column an
// invalid ID, which is how an entity a pattern did not match is spelled. One per column
// kind, selected during translation the way the gather and append families are.
using NLFillNullFunction = void (*)(Column* output, size_t rowCount);

}
