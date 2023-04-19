#pragma once

#include <boost/lexical_cast.hpp>

template <typename NumberType>
NumberType StringToNumber(std::string_view str, bool& error) {
    try {
        const NumberType res = boost::lexical_cast<NumberType>(str.data(), str.size());
        error = false;
        return res;
    } catch (const boost::bad_lexical_cast& e) {
        error = true;
        return 0;
    }
}
