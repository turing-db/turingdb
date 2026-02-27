#include "EntityList.h"

using namespace db;

EntityList::EntityList() = default;

EntityList::~EntityList() = default;

EntityList::EntityList(const EntityList& other) = default;

EntityList::EntityList(EntityList&& other) noexcept = default;

EntityList& EntityList::operator=(const EntityList& other) = default;

EntityList& EntityList::operator=(EntityList&& other) noexcept = default;
