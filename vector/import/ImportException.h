#pragma once

#include "ImportResult.h"

#include "TuringException.h"

namespace vec {

class ImportException : public TuringException {
public:
    ImportException(ImportErrorCode code, const std::string& message = "");
    ~ImportException() noexcept override;

    const ImportError& err() const {
        return _e;
    }

    ImportError&& err() {
        return std::move(_e);
    }

private:
    ImportError _e;
};

}
