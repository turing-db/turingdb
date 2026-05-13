#include "BaseConnectionState.h"

namespace net {

BaseConnectionState::BaseConnectionState() {
}

BaseConnectionState::~BaseConnectionState() {
}

void BaseConnectionState::init(CreateAbstractTCPWriterFunc writerFunc,
                               CreateAbstractTCPParserFunc parserFunc,
                               NetBuffer* buffer) {
    _writer = writerFunc(this);
    _parser = parserFunc(buffer, this);
}

}
