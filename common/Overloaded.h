#pragma once

template<class... Ts>
struct Overloaded : Ts... { using Ts::operator()...; };
