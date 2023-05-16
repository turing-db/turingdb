#include "AuthGuard.h"

#include <string_view>

#include "crow.h"

#include "AuthRegister.h"

#define AUTH_ID_KEY "auth_id"
#define AUTH_SECRET_KEY "auth_secret"

void AuthGuard::setRegister(const AuthRegister* authRegister) {
    _authRegister = authRegister;
}

void AuthGuard::before_handle(crow::request& req,
                              crow::response& res,
                              context& ctxt) {
    const auto msg = crow::json::load(req.body);
    if (!msg) {
        res.code = 400;
        res.end();
        return;
    }

    if (!msg.has(AUTH_ID_KEY) || !msg.has(AUTH_SECRET_KEY)) {
        res.code = 403;
        res.end();
        return;
    }

    const auto authID = msg[AUTH_ID_KEY].s();
    const auto authSecret = msg[AUTH_SECRET_KEY].s();

    const auto idSize = authID.size();
    const auto secretSize = authSecret.size();

    if (idSize == 0 || idSize > MAX_ID_SIZE || secretSize == 0 || secretSize > MAX_SECRET_SIZE) {
        res.code = 403;
        res.end();
        return;
    }

    if (!_authRegister || !_authRegister->isValid(authID, authSecret)) {
        res.code = 403;
        res.end();
        return;
    }
}

void AuthGuard::after_handle(crow::request& req,
                             crow::response& res,
                             context& ctxt) {
}
