package main

import (
	"reflect"
	"testing"
)

func TestShellSplit(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{``, nil},
		{`   `, nil},
		{`a b c`, []string{"a", "b", "c"}},
		{`sudo -u postgres -E /tmp/pgsafe worker stdio`,
			[]string{"sudo", "-u", "postgres", "-E", "/tmp/pgsafe", "worker", "stdio"}},
		{`-p 2222 -i ~/.ssh/id_ed25519`,
			[]string{"-p", "2222", "-i", "~/.ssh/id_ed25519"}},
		{`"a b" c`, []string{"a b", "c"}},
		{`'a b' c`, []string{"a b", "c"}},
		{`-o "UserKnownHostsFile=/tmp/k h"`,
			[]string{"-o", "UserKnownHostsFile=/tmp/k h"}},
		{`a\ b`, []string{"a b"}},
		{`"a\"b"`, []string{`a"b`}},
	}
	for _, tc := range cases {
		got, err := shellSplit(tc.in)
		if err != nil {
			t.Errorf("shellSplit(%q) error: %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("shellSplit(%q) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
}

func TestShellSplitUnmatched(t *testing.T) {
	for _, in := range []string{`"abc`, `'abc`, `"a\"`} {
		if _, err := shellSplit(in); err == nil {
			t.Errorf("shellSplit(%q): want error, got nil", in)
		}
	}
}
