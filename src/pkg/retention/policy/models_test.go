package policy

import (
	"fmt"
	"testing"

	"github.com/beego/beego/v2/core/validation"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/pkg/retention/policy/rule"
)

func TestAlgorithm(t *testing.T) {
	p := Metadata{
		Algorithm: "bad",
	}
	v := &validation.Validation{}
	ok, err := v.Valid(p)
	require.Nil(t, err)
	require.False(t, ok)
}

// beego 1.9.0 has bug to deal with nil interface
// func TestTrigger(t *testing.T) {
// 	p := Metadata{
// 		Algorithm: "or",
// 	}
// 	v := &validation.Validation{}
// 	ok, err := v.Valid(p)
//
// 	require.Nil(t, err)
// 	require.False(t, ok)
// 	for _, e := range v.Errors {
// 		fmt.Print(e.Field, e.Message)
// 	}
// }

type PolicyTestSuite struct {
	suite.Suite

	policy *Metadata
}

// TestRetentionPolicy is the entry method of running PolicyTestSuite.
func TestRetentionPolicy(t *testing.T) {
	suite.Run(t, &PolicyTestSuite{})
}

// SetupSuite prepares the env for PolicyTestSuite.
func (p *PolicyTestSuite) SetupSuite() {
	p.policy = &Metadata{}
	p.policy.Trigger = &Trigger{}
}

// TearDownSuite clears the env for PolicyTestSuite.
func (p *PolicyTestSuite) TearDownSuite() {
	p.policy = nil
}

func (p *PolicyTestSuite) TestValidateRetentionPolicy() {
	p.policy.Trigger.Kind = TriggerKindSchedule

	// cron is not in the map of trigger setting
	p.NoError(p.policy.ValidateRetentionPolicy())

	// cron value is an empty string
	p.policy.Trigger.Settings = map[string]any{"cron": ""}
	p.NoError(p.policy.ValidateRetentionPolicy())

	//  the 1st field of cron value is not 0
	p.policy.Trigger.Settings = map[string]any{"cron": "1 0 0 1 1 *"}
	p.Error(p.policy.ValidateRetentionPolicy())

	// valid cron value
	p.policy.Trigger.Settings = map[string]any{"cron": "0 0 0 1 1 *"}
	p.NoError(p.policy.ValidateRetentionPolicy())
}

func TestValidateMaxDeletionsPerRepository(t *testing.T) {
	newPolicy := func(max *int) *Metadata {
		return &Metadata{
			Algorithm:                 AlgorithmOR,
			MaxDeletionsPerRepository: max,
			Trigger: &Trigger{
				Kind:     "Manual",
				Settings: map[string]any{},
			},
			Scope: &Scope{
				Level:     ScopeLevelProject,
				Reference: 1,
			},
		}
	}

	// omitted means no threshold
	require.NoError(t, newPolicy(nil).ValidateRetentionPolicy())

	// zero is a valid threshold
	require.NoError(t, newPolicy(ptrInt(0)).ValidateRetentionPolicy())

	// positive threshold is valid
	require.NoError(t, newPolicy(ptrInt(10)).ValidateRetentionPolicy())

	// negative threshold is rejected with a bad request error
	err := newPolicy(ptrInt(-1)).ValidateRetentionPolicy()
	require.Error(t, err)
	require.Equal(t, errors.BadRequestCode, errors.ErrCode(err))
}

func ptrInt(n int) *int {
	return &n
}

func TestRule(t *testing.T) {
	p := &Metadata{
		Algorithm: "or",
		Rules: []rule.Metadata{
			{
				ID:       1,
				Priority: 1,
				Action:   "retain",
				Template: "latestPushedK",
				Parameters: rule.Parameters{
					"latestPushedK": 10,
				},
				TagSelectors: []*rule.Selector{
					{
						Kind:       "badkind", // validate doublestar
						Decoration: "matches",
						Pattern:    "release-[\\d\\.]+",
					},
				},
				ScopeSelectors: map[string][]*rule.Selector{
					"repository": {
						{
							Kind:       "doublestar",
							Decoration: "matches",
							Pattern:    ".+",
						},
					},
				},
			},
		},
		Trigger: &Trigger{
			Kind: "Schedule",
			Settings: map[string]any{
				"cron": "* 22 11 * * *",
			},
		},
		Scope: &Scope{
			Level:     "project",
			Reference: 1,
		},
	}
	v := &validation.Validation{}
	ok, err := v.Valid(p)

	require.Nil(t, err)
	require.False(t, ok)
	require.True(t, v.HasErrors())
	require.EqualValues(t, "Kind", v.Errors[0].Field)
	for _, e := range v.Errors {
		fmt.Printf("%s %s\n", e.Field, e.Message)
	}
}

func TestParamValid(t *testing.T) {
	p := &Metadata{
		Algorithm: "or",
		Rules: []rule.Metadata{
			{
				ID:       1,
				Priority: 1,
				Action:   "retain",
				Template: "latestPushedK",
				Parameters: rule.Parameters{
					"latestPushedK": -10,
				},
				TagSelectors: []*rule.Selector{
					{
						Kind:       "doublestar",
						Decoration: "matches",
						Pattern:    "release-[\\d\\.]+",
					},
				},
				ScopeSelectors: map[string][]*rule.Selector{
					"repository": {
						{
							Kind:       "doublestar",
							Decoration: "matches",
							Pattern:    ".+",
						},
					},
				},
			},
		},
		Trigger: &Trigger{
			Kind: "Schedule",
			Settings: map[string]any{
				"cron": "* 22 11 * * *",
			},
		},
		Scope: &Scope{
			Level:     "project",
			Reference: 1,
		},
	}
	v := &validation.Validation{}
	ok, err := v.Valid(p)
	require.Nil(t, err)
	require.False(t, ok)
	require.True(t, v.HasErrors())
	require.EqualValues(t, "Parameters", v.Errors[0].Field)

	p = &Metadata{
		Algorithm: "or",
		Rules: []rule.Metadata{
			{
				ID:       1,
				Priority: 1,
				Action:   "retain",
				Template: "nDaysSinceLastPull",
				Parameters: rule.Parameters{
					"nDaysSinceLastPull": 20201010,
				},
				TagSelectors: []*rule.Selector{
					{
						Kind:       "doublestar",
						Decoration: "matches",
						Pattern:    "release-[\\d\\.]+",
					},
				},
				ScopeSelectors: map[string][]*rule.Selector{
					"repository": {
						{
							Kind:       "doublestar",
							Decoration: "matches",
							Pattern:    ".+",
						},
					},
				},
			},
		},
		Trigger: &Trigger{
			Kind: "Schedule",
			Settings: map[string]any{
				"cron": "* 22 11 * * *",
			},
		},
		Scope: &Scope{
			Level:     "project",
			Reference: 1,
		},
	}
	v = &validation.Validation{}
	ok, err = v.Valid(p)
	require.Nil(t, err)
	require.False(t, ok)
	require.True(t, v.HasErrors())
	require.EqualValues(t, "Parameters", v.Errors[0].Field)
}
