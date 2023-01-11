package peer

import (
	"crypto/rsa"
)


// Crypting describes functions to encrypt and decrypt data.
type Crypting interface {
	VerifySignature(signature []byte, hash []byte, pubkey *rsa.PublicKey) bool

	SignHash(hash []byte, privkey *rsa.PrivateKey) []byte

	ComputeHashKeyForList(list []int) []byte

	GetPrivateKey() *rsa.PrivateKey

	GetPublicKey() *rsa.PublicKey
}
