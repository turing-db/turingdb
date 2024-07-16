#pragma once

template<class... Ts> struct lambdas : Ts... { using Ts::operator()...; };
template<class... Ts> lambdas(Ts...) -> lambdas<Ts...>;
