// Package encryption is a thin wrapper around filippo.io/age for the filter
// chain. age does the cryptographic work; we expose a fixed surface so the
// chain composition () doesn't take a
// direct dep on the age types beyond the recipient/identity interfaces.
package encryption

import (
	"errors"
	"fmt"
	"io"

	"filippo.io/age"
)

// NewWriter encrypts to sink for the given recipients. Closing the returned
// writer finalizes the age stream — it does not close sink.
func NewWriter(sink io.Writer, recipients []age.Recipient) (io.WriteCloser, error) {
	if len(recipients) == 0 {
		return nil, errors.New("encryption: at least one recipient required")
	}
	wc, err := age.Encrypt(sink, recipients...)
	if err != nil {
		return nil, fmt.Errorf("encryption: age.Encrypt: %w", err)
	}
	return wc, nil
}

// NewReader decrypts src using the supplied identities. Returns the first
// identity whose key matches the stream's recipients (age handles selection
// internally).
func NewReader(src io.Reader, identities []age.Identity) (io.Reader, error) {
	if len(identities) == 0 {
		return nil, errors.New("encryption: at least one identity required")
	}
	r, err := age.Decrypt(src, identities...)
	if err != nil {
		return nil, fmt.Errorf("encryption: age.Decrypt: %w", err)
	}
	return r, nil
}
