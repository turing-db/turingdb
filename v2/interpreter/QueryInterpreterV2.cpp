#include "QueryInterpreterV2.h"

#include "InterpreterContext.h"
#include "SystemManager.h"
#include "JobSystem.h"

#include "Profiler.h"

using namespace db;

QueryInterpreterV2::QueryInterpreterV2(SystemManager* sysMan,
                                       JobSystem* jobSystem)
    : _sysMan(sysMan),
    _jobSystem(jobSystem)
{
}

QueryInterpreterV2::~QueryInterpreterV2() {
}

QueryStatus QueryInterpreterV2::execute(const InterpreterContext& ctxt,
                                        std::string_view query,
                                        std::string_view graphName) {
    Profile profile {"QueryInterpreterV2::execute"};

    return QueryStatus(QueryStatus::Status::OK);
}
