package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

const signatureVersion = "v1="

// Sign returns the value of the X-Wingpost-Signature header.
// The signed text is the Unix timestamp, a ".", and the request body.
func Sign(secret string, sentAt time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(sentAt.Unix(), 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return signatureVersion + hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether a signature header matches the body and timestamp.
// A receiver calls Verify before it trusts a delivery.
func Verify(secret, header string, sentAt time.Time, body []byte) bool {
	want := Sign(secret, sentAt, body)
	return hmac.Equal([]byte(want), []byte(header))
}
