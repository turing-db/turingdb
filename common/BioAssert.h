#pragma once

#ifdef NDEBUG
#define bioassert(C) (void)(C)
#define msgbioassert(C, msg) (void)(C)
#else
void __bioAssertWithLocation(const char* file, unsigned line, const char* expr, const char* msg = "");

#define bioassert(C) ({ if (!(C)) { __bioAssertWithLocation(__FILE__, __LINE__, #C); }})
#define msgbioassert(C, msg) ({ if (!(C)) { __bioAssertWithLocation(__FILE__, __LINE__, #C, msg); }})
#endif
