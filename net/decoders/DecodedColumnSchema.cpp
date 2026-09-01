#include "DecodedColumnSchema.h"

using namespace net::proto;

ProtoColumnState::ProtoColumnState() = default;
ProtoColumnState::ProtoColumnState(const ProtoColumnState&) = default;
ProtoColumnState::ProtoColumnState(ProtoColumnState&&) = default;
ProtoColumnState& ProtoColumnState::operator=(const ProtoColumnState&) = default;
ProtoColumnState& ProtoColumnState::operator=(ProtoColumnState&&) = default;
ProtoColumnState::~ProtoColumnState() = default;

DecodedColumnSchema::DecodedColumnSchema() = default;
DecodedColumnSchema::DecodedColumnSchema(const DecodedColumnSchema&) = default;
DecodedColumnSchema::DecodedColumnSchema(DecodedColumnSchema&&) = default;
DecodedColumnSchema& DecodedColumnSchema::operator=(const DecodedColumnSchema&) = default;
DecodedColumnSchema& DecodedColumnSchema::operator=(DecodedColumnSchema&&) = default;
DecodedColumnSchema::~DecodedColumnSchema() = default;
