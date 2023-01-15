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

/*
// rewrites the above function but instead of []int use map[String]int
func (n *node) ComputeHashKeyForMap(m map[string]int) []byte {
	hash := crypto.SHA256.New()

	b := make([]byte, 0)

	for k, v := range m {
		b = append(b, []byte(k+strconv.Itoa(v))...)
	}
	_, err := hash.Write(b)
	if err != nil {
		panic(err)
	}

	return hash.Sum(nil)
}
*/

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

/*
// PrivateKeyToBytes private key to bytes
func PrivateKeyToBytes(priv *rsa.PrivateKey) []byte {
	privBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(priv),
		},
	)

	return privBytes
}

// PublicKeyToBytes public key to bytes
func PublicKeyToBytes(pub *rsa.PublicKey) []byte {
	pubASN1, _ := x509.MarshalPKIXPublicKey(pub)

	pubBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: pubASN1,
	})

	return pubBytes
}

// BytesToPrivateKey bytes to private key
func BytesToPrivateKey(priv []byte) *rsa.PrivateKey {
	block, _ := pem.Decode(priv)
	end := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	var err error
	// test if is encrypted pem block

	if enc {
		log.Println("is encrypted pem block")
		b, err = x509.DecryptPEMBlock(block, nil)
		if err != nil {
			log.Error(err)
		}
	}
	key, err := x509.ParsePKCS1PrivateKey(b)
	if err != nil {
		log.Error(err)
	}
	return key
}

// BytesToPublicKey bytes to public key
func BytesToPublicKey(pub []byte) *rsa.PublicKey {
	block, _ := pem.Decode(pub)
	enc := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	var err error
	if enc {
		log.Println("is encrypted pem block")
		b, err = x509.DecryptPEMBlock(block, nil)
		if err != nil {
			xerrors.Errorf(err)
			log.Error(err)
		}
	}
	ifc, err := x509.ParsePKIXPublicKey(b)
	if err != nil {
		log.Error(err)
	}
	key, ok := ifc.(*rsa.PublicKey)
	if !ok {
		log.Error("not ok")
	}
	return key
}

// EncryptWithPublicKey encrypts data with public key
func EncryptWithPublicKey(msg []byte, pub *rsa.PublicKey) []byte {
	hash := sha512.New()
	ciphertext, err := rsa.EncryptOAEP(hash, rand.Reader, pub, msg, nil)
	if err != nil {
		log.Error(err)
	}
	return ciphertext
}

// DecryptWithPrivateKey decrypts data with private key
func DecryptWithPrivateKey(ciphertext []byte, priv *rsa.PrivateKey) []byte {
	hash := sha512.New()
	plaintext, err := rsa.DecryptOAEP(hash, rand.Reader, priv, ciphertext, nil)
	if err != nil {
		log.Error(err)
	}
	return plaintext
}

*/
