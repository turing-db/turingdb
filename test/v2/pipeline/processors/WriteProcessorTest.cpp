#include <gtest/gtest.h>

#include <range/v3/view/zip.hpp>

#include "EdgeRecord.h"
#include "ID.h"
#include "LineContainer.h"
#include "dataframe/ColumnTag.h"
#include "iterators/ChunkConfig.h"
#include "processors/ProcessorTester.h"

#include "processors/WriteProcessor.h"
#include "dataframe/Dataframe.h"
#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "columns/ColumnIDs.h"
#include "SystemManager.h"
#include "SimpleGraph.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;
namespace rg = ranges;
namespace rv = rg::views;

class WriteProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};


