#include <openssl/rand.h>

#include <vector>
#include <memory>

class AESGCMEncryptor {
public:
    explicit AESGCMEncryptor()
    :_key(KEY_SIZE)
    {
        if (RAND_bytes(_key.data(), KEY_SIZE) != 1) {
            throw std::runtime_error("Failed to generate key");
        }
    }

    explicit AESGCMEncryptor(const std::vector<unsigned char>& existing_key)
    :_key(existing_key)
    {
        if (_key.size() != KEY_SIZE) {
            throw std::runtime_error("Invalid key size");
        }
    }

    std::string encrypt(const std::string& plaintext);

    std::string decrypt(const std::string& encrypted_data);

private:
    std::vector<unsigned char> _key;
    static constexpr int KEY_SIZE = 32; // AES-256
    static constexpr int IV_SIZE = 12;  // 96-bit IV for GCM
    static constexpr int TAG_SIZE = 16; // 128-bit authentication tag
};
