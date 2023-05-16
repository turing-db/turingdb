#pragma once

namespace crow {
class request;
class response;
}

class AuthRegister;

class AuthGuard {
public:
    struct context {
    };

    void setRegister(const AuthRegister* authRegister);

    void before_handle(crow::request& req,
                       crow::response& res,
                       context& ctxt);
    void after_handle(crow::request& req,
                      crow::response& res,
                      context& ctxt);

private:
    static constexpr unsigned MAX_ID_SIZE {128};
    static constexpr unsigned MAX_SECRET_SIZE {128};
    const AuthRegister* _authRegister {nullptr};
};
