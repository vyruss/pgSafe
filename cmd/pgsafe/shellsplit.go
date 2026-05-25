package main

import (
	"fmt"
	"strings"
)

// shellSplit tokenises a POSIX-ish shell line into argv. Used by --remote-command
// and --ssh-extra-args so operators write one quoted string instead of one flag
// per argv element. Supports double quotes, single quotes, and backslash escapes;
// unmatched quotes return an error. No glob, no $var expansion — flags carry
// already-resolved values.
func shellSplit(s string) ([]string, error) {
	var out []string
	var cur strings.Builder
	inSingle, inDouble, inToken := false, false, false
	flush := func() {
		if inToken {
			out = append(out, cur.String())
			cur.Reset()
			inToken = false
		}
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			} else {
				cur.WriteByte(c)
			}
		case inDouble:
			switch c {
			case '"':
				inDouble = false
			case '\\':
				if i+1 < len(s) {
					n := s[i+1]
					if n == '"' || n == '\\' || n == '$' || n == '`' || n == '\n' {
						cur.WriteByte(n)
						i++
					} else {
						cur.WriteByte(c)
					}
				}
			default:
				cur.WriteByte(c)
			}
		default:
			switch c {
			case ' ', '\t', '\n':
				flush()
			case '\'':
				inSingle, inToken = true, true
			case '"':
				inDouble, inToken = true, true
			case '\\':
				if i+1 < len(s) {
					cur.WriteByte(s[i+1])
					inToken = true
					i++
				}
			default:
				cur.WriteByte(c)
				inToken = true
			}
		}
	}
	if inSingle || inDouble {
		return nil, fmt.Errorf("unmatched quote")
	}
	flush()
	return out, nil
}
