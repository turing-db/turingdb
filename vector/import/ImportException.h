#pragma once

#include "ImportResult.h"

namespace vec {

class ImportException : public std::exception {
public:
    ImportException(ImportError&& e)
        : _e(std::move(e))
    {
    }

    ImportException(ImportErrorCode code, std::string message = "")
        : _e(ImportError(code, std::move(message)))
    {
    }

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
