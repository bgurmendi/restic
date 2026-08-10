package main

import (
	"testing"

	"github.com/restic/restic/internal/data"
	rtest "github.com/restic/restic/internal/test"
	"github.com/spf13/pflag"
)

func TestForgetPolicyValues(t *testing.T) {
	testCases := []struct {
		input string
		value ForgetPolicyCount
		err   string
	}{
		{"0", ForgetPolicyCount(0), ""},
		{"1", ForgetPolicyCount(1), ""},
		{"unlimited", ForgetPolicyCount(-1), ""},
		{"", ForgetPolicyCount(0), "strconv.ParseInt: parsing \"\": invalid syntax"},
		{"-1", ForgetPolicyCount(0), ErrNegativePolicyCount.Error()},
		{"abc", ForgetPolicyCount(0), "strconv.ParseInt: parsing \"abc\": invalid syntax"},
	}
	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			var count ForgetPolicyCount
			err := count.Set(testCase.input)

			if testCase.err != "" {
				rtest.Assert(t, err != nil, "should have returned error for input %+v", testCase.input)
				rtest.Equals(t, testCase.err, err.Error())
			} else {
				rtest.Assert(t, err == nil, "expected no error for input %+v, got %v", testCase.input, err)
				rtest.Equals(t, testCase.value, count)
				rtest.Equals(t, testCase.input, count.String())
			}
		})
	}
}

func TestForgetOptionValues(t *testing.T) {
	const negValErrorMsg = "Fatal: negative values other than -1 are not allowed for --keep-*"
	const negDurationValErrorMsg = "Fatal: durations containing negative values are not allowed for --keep-within*"
	testCases := []struct {
		input    ForgetOptions
		errorMsg string
	}{
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Last: 1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Hourly: 1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Daily: 1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Weekly: 1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Monthly: 1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Yearly: 1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Last: 0}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Hourly: 0}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Daily: 0}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Weekly: 0}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Monthly: 0}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Yearly: 0}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Last: -1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Hourly: -1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Daily: -1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Weekly: -1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Monthly: -1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Yearly: -1}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Last: -2}}, negValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Hourly: -2}}, negValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Daily: -2}}, negValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Weekly: -2}}, negValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Monthly: -2}}, negValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Yearly: -2}}, negValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Within: data.ParseDurationOrPanic("1y2m3d3h")}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinHourly: data.ParseDurationOrPanic("1y2m3d3h")}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinDaily: data.ParseDurationOrPanic("1y2m3d3h")}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinWeekly: data.ParseDurationOrPanic("1y2m3d3h")}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinMonthly: data.ParseDurationOrPanic("2y4m6d8h")}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinYearly: data.ParseDurationOrPanic("2y4m6d8h")}}, ""},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{Within: data.ParseDurationOrPanic("-1y2m3d3h")}}, negDurationValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinHourly: data.ParseDurationOrPanic("1y-2m3d3h")}}, negDurationValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinDaily: data.ParseDurationOrPanic("1y2m-3d3h")}}, negDurationValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinWeekly: data.ParseDurationOrPanic("1y2m3d-3h")}}, negDurationValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinMonthly: data.ParseDurationOrPanic("-2y4m6d8h")}}, negDurationValErrorMsg},
		{ForgetOptions{PolicySelectionOptions: PolicySelectionOptions{WithinYearly: data.ParseDurationOrPanic("2y-4m6d8h")}}, negDurationValErrorMsg},
	}

	for _, testCase := range testCases {
		err := verifyForgetOptions(&testCase.input)
		if testCase.errorMsg != "" {
			rtest.Assert(t, err != nil, "should have returned error for input %+v", testCase.input)
			rtest.Equals(t, testCase.errorMsg, err.Error())
		} else {
			rtest.Assert(t, err == nil, "expected no error for input %+v", testCase.input)
		}
	}
}

func TestForgetHostnameDefaulting(t *testing.T) {
	t.Setenv("RESTIC_HOST", "testhost")

	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "env default when flag not set",
			args: nil,
			want: []string{"testhost"},
		},
		{
			name: "flag overrides env",
			args: []string{"--host", "flaghost"},
			want: []string{"flaghost"},
		},
		{
			name: "empty flag clears env",
			args: []string{"--host", ""},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			set := pflag.NewFlagSet(tt.name, pflag.ContinueOnError)
			opts := ForgetOptions{}
			opts.AddFlags(set)
			err := set.Parse(tt.args)
			rtest.Assert(t, err == nil, "expected no error for input")
			finalizeSnapshotFilter(&opts.SnapshotFilter)
			rtest.Equals(t, tt.want, opts.Hosts)
		})
	}
}
