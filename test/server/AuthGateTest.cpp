#include <gtest/gtest.h>

#include "AuthGate.h"
#include "Authenticator.h"
#include "HTTPParsingInfo.h"

using namespace db;

// -------------------------------------------------------------------
// extractBearerToken
// -------------------------------------------------------------------

TEST(ExtractBearerToken, ReturnsTokenAfterScheme) {
    EXPECT_EQ(extractBearerToken("Bearer abc123"), "abc123");
}

TEST(ExtractBearerToken, SchemeIsCaseInsensitive) {
    EXPECT_EQ(extractBearerToken("bearer abc123"), "abc123");
    EXPECT_EQ(extractBearerToken("BEARER abc123"), "abc123");
    EXPECT_EQ(extractBearerToken("BeArEr abc123"), "abc123");
}

TEST(ExtractBearerToken, TrimsSurroundingWhitespace) {
    EXPECT_EQ(extractBearerToken("Bearer    abc123"), "abc123");
    EXPECT_EQ(extractBearerToken("Bearer abc123   "), "abc123");
    EXPECT_EQ(extractBearerToken("Bearer \tabc123\t "), "abc123");
}

TEST(ExtractBearerToken, TrimsTrailingCarriageReturn) {
    EXPECT_EQ(extractBearerToken("Bearer abc123\r"), "abc123");
}

TEST(ExtractBearerToken, NonBearerSchemeYieldsEmpty) {
    EXPECT_TRUE(extractBearerToken("Basic abc123").empty());
    EXPECT_TRUE(extractBearerToken("Token abc123").empty());
}

TEST(ExtractBearerToken, MissingSpaceAfterSchemeYieldsEmpty) {
    EXPECT_TRUE(extractBearerToken("Bearerabc123").empty());
}

TEST(ExtractBearerToken, EmptyHeaderYieldsEmpty) {
    EXPECT_TRUE(extractBearerToken("").empty());
}

TEST(ExtractBearerToken, SchemeWithoutTokenYieldsEmpty) {
    EXPECT_TRUE(extractBearerToken("Bearer").empty());
    EXPECT_TRUE(extractBearerToken("Bearer ").empty());
}

TEST(ExtractBearerToken, WhitespaceOnlyTokenYieldsEmpty) {
    EXPECT_TRUE(extractBearerToken("Bearer     ").empty());
}

// -------------------------------------------------------------------
// isRequestAuthorized
// -------------------------------------------------------------------

TEST(IsRequestAuthorized, NullAuthenticatorAllowsEverything) {
    const net::HTTP::Info info;
    EXPECT_TRUE(isRequestAuthorized(nullptr, info));
}

TEST(IsRequestAuthorized, DisabledAuthenticatorAllowsEverything) {
    Authenticator authenticator("");
    ASSERT_FALSE(authenticator.isEnabled());

    const net::HTTP::Info info;
    EXPECT_TRUE(isRequestAuthorized(&authenticator, info));
}

TEST(IsRequestAuthorized, EnabledAuthenticatorRejectsMissingHeader) {
    Authenticator authenticator("secret-key");
    ASSERT_TRUE(authenticator.isEnabled());

    const net::HTTP::Info info;
    EXPECT_FALSE(isRequestAuthorized(&authenticator, info));
}

TEST(IsRequestAuthorized, EnabledAuthenticatorAcceptsCorrectKey) {
    Authenticator authenticator("secret-key");

    const net::HTTP::Info info("Bearer secret-key");
    EXPECT_TRUE(isRequestAuthorized(&authenticator, info));
}

TEST(IsRequestAuthorized, EnabledAuthenticatorRejectsWrongKey) {
    Authenticator authenticator("secret-key");

    const net::HTTP::Info info("Bearer wrong-key");
    EXPECT_FALSE(isRequestAuthorized(&authenticator, info));
}

TEST(IsRequestAuthorized, EnabledAuthenticatorRejectsNonBearerScheme) {
    Authenticator authenticator("secret-key");

    const net::HTTP::Info info("Basic secret-key");
    EXPECT_FALSE(isRequestAuthorized(&authenticator, info));
}

TEST(IsRequestAuthorized, EnabledAuthenticatorAcceptsCaseInsensitiveSchemeWithWhitespace) {
    Authenticator authenticator("secret-key");

    const net::HTTP::Info info("bearer    secret-key \r");
    EXPECT_TRUE(isRequestAuthorized(&authenticator, info));
}
