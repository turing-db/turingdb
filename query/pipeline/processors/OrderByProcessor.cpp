#include "OrderByProcessor.h"

using namespace db;

OrderByProcessor::OrderByProcessor()
{
}

OrderByProcessor::~OrderByProcessor() {
}

void OrderByProcessor::prepare(ExecutionContext* /*ctxt*/) {
    markAsPrepared();
}

void OrderByProcessor::reset() {
    markAsReset();
}

void OrderByProcessor::execute() {
}
