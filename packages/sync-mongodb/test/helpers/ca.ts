/**
 * A self-signed certificate for tests that need a value `X509Certificate`
 * accepts. Generated once with `openssl req -x509 -newkey ec ...` and its
 * private key discarded on the spot, so it can sign nothing and is not a
 * secret; it names no real host and is trusted by nothing.
 */
export const TEST_CA_PEM = `-----BEGIN CERTIFICATE-----
MIIBdTCCARygAwIBAgIUYA/j8vmXLoOxBg17MVsxr0WptZgwCgYIKoZIzj0EAwIw
IDEeMBwGA1UEAwwVc3luYy1wb3N0Z3JlcyB0ZXN0IENBMCAXDTI2MTAyOTE2NDE1
OVoYDzIxMjYxMDA1MTY0MTU5WjAgMR4wHAYDVQQDDBVzeW5jLXBvc3RncmVzIHRl
c3QgQ0EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAR/tIk0cVsFVh49bOYbZ2UB
blKkXKfCfgj3+6A5B7t+dz+phLfY0PJe/RfKCPnbvQI4ZDO8jkLctAjItiiNjbBm
ozIwMDAdBgNVHQ4EFgQUdosu7nttrBB5We4MUhtGRm7Q/gEwDwYDVR0TAQH/BAUw
AwEB/zAKBggqhkjOPQQDAgNHADBEAiBWaRIW/Ef4zN9nmK01dsPVor+p06ihoxO8
G/oBUsR6zAIgOJSTKCW/ymLt0xjT42M7xm2AoJximsh0cy3Wbk+mohU=
-----END CERTIFICATE-----
`
