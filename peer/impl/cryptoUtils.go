package impl

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"sort"
	"strconv"
)

// GenerateKeyPair generates a new key pair
func GenerateKeyPair(bits int) (*rsa.PrivateKey, *rsa.PublicKey) {
	privkey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		panic(err)
	}
	return privkey, &privkey.PublicKey
}

func (n *node) VerifySignature(signature []byte, hash []byte, pubkey *rsa.PublicKey) bool {
	err := rsa.VerifyPSS(pubkey, crypto.SHA256, hash, signature, nil)
	return err == nil
}

func (n *node) SignHash(hash []byte, privkey *rsa.PrivateKey) []byte {
	signature, err := rsa.SignPSS(rand.Reader, privkey, crypto.SHA256, hash, nil)
	if err != nil {
		panic(err)
	}
	return signature
}

func (n *node) ComputeHashKeyForList(list []int) []byte {
	hash := crypto.SHA256.New()

	for _, v := range list {
		_, err := hash.Write([]byte(strconv.Itoa(v)))
		if err != nil {
			panic(err)
		}
	}

	return hash.Sum(nil)
}

func (n *node) ComputeHashKeyForMap(m map[string]int) []byte {
	// Create a new hash
	hash := sha256.New()

	// Create a byte slice to hold the key-value pairs
	pairs := make([]byte, 0)

	// Get the keys of the map
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Sort the keys
	sort.Strings(keys)

	// Iterate through the map and append the key-value pairs to the slice
	for _, k := range keys {
		v := m[k]
		pairs = append(pairs, []byte(k+strconv.Itoa(v))...)
	}

	// Write the slice to the hash
	hash.Write(pairs)

	// Return the sum of the hash
	return hash.Sum(nil)
}

func (n *node) GetPrivateKey() *rsa.PrivateKey {
	return n.PrivateKey
}

func (n *node) GetPublicKey() *rsa.PublicKey {
	return n.PublicKey
}
