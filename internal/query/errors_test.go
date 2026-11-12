package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestAPIError_Error(t *testing.T) {
	e := &APIError{Code: "unbounded_scan", Message: "query lacks a time bound"}
	got := e.Error()
	want := "unbounded_scan: query lacks a time bound"
	if got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestAPIError_JSON(t *testing.T) {
	e := &APIError{
		Code:            "unknown_field",
		Message:         "field 'foo' not found",
		Hint:            "did you mean 'bar'?",
		SuggestedFields: []string{"bar", "baz"},
		Retryable:       false,
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got["code"] != "unknown_field" {
		t.Errorf("code = %v, want unknown_field", got["code"])
	}
	if got["message"] != "field 'foo' not found" {
		t.Errorf("message = %v", got["message"])
	}
	if got["hint"] != "did you mean 'bar'?" {
		t.Errorf("hint = %v", got["hint"])
	}
	sf, ok := got["suggested_fields"].([]any)
	if !ok || len(sf) != 2 {
		t.Fatalf("suggested_fields = %v", got["suggested_fields"])
	}
	if sf[0] != "bar" || sf[1] != "baz" {
		t.Errorf("suggested_fields = %v", sf)
	}
	if got["retryable"] != false {
		t.Errorf("retryable = %v, want false", got["retryable"])
	}
}

func TestAPIError_JSON_OmitsEmpty(t *testing.T) {
	e := &APIError{Code: "budget_exceeded", Message: "over budget"}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := got["hint"]; ok {
		t.Error("hint should be omitted when empty")
	}
	if _, ok := got["suggested_fields"]; ok {
		t.Error("suggested_fields should be omitted when empty")
	}
}

func TestAPIError_ErrorsAs(t *testing.T) {
	orig := &APIError{Code: "snapshot_expired", Message: "snapshot too old"}
	wrapped := fmt.Errorf("handler failed: %w", orig)

	var target *APIError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As did not find *APIError")
	}
	if target.Code != "snapshot_expired" {
		t.Errorf("Code = %q, want snapshot_expired", target.Code)
	}
}

func TestNewAPIError(t *testing.T) {
	e := NewAPIError("unbounded_scan", "needs time bound")
	if e.Code != "unbounded_scan" {
		t.Errorf("Code = %q", e.Code)
	}
	if e.Message != "needs time bound" {
		t.Errorf("Message = %q", e.Message)
	}
}

func TestAPIError_FluentBuilders(t *testing.T) {
	e := NewAPIError("unknown_field", "bad field").
		WithHint("try 'bar'").
		WithSuggestedFields([]string{"bar"})

	if e.Hint != "try 'bar'" {
		t.Errorf("Hint = %q", e.Hint)
	}
	if len(e.SuggestedFields) != 1 || e.SuggestedFields[0] != "bar" {
		t.Errorf("SuggestedFields = %v", e.SuggestedFields)
	}
}

func TestErrorCodeConstants(t *testing.T) {
	codes := []string{
		ErrCodeUnboundedScan,
		ErrCodeUnknownField,
	}
	want := []string{
		"unbounded_scan",
		"unknown_field",
	}
	for i, c := range codes {
		if c != want[i] {
			t.Errorf("code[%d] = %q, want %q", i, c, want[i])
		}
	}
}

// TestRenewMemBudget: a renewed budget is whole again while the one it renews stays spent.
func TestRenewMemBudget(t *testing.T) {
	ctx := WithMemBudget(context.Background(), 10)
	if err := errors.Join(ChargeMem(ctx, 8), ChargeMem(RenewMemBudget(ctx), 8)); err != nil {
		t.Errorf("charge 8 of 10, then 8 of a renewed 10: %v", err)
	}
	var apiErr *APIError
	if !errors.As(ChargeMem(ctx, 8), &apiErr) || apiErr.Code != ErrCodeTooLarge {
		t.Errorf("charge past the spent budget = %v, want too_large", apiErr)
	}
}

// TestChargeMemWaits: a read's charge that does not fit waits for a working goroutine to
// give memory back, a later turn's past half the budget for the earlier turns to end, and a
// wait ends with its context; with nothing working to free room, it fails too_large at once.
func TestChargeMemWaits(t *testing.T) {
	tooLarge := NewAPIError(ErrCodeTooLarge, "")
	tests := []struct {
		name string
		// hold spends some of the budget before the read charges 5 of 10, and returns what
		// lets the read go on once it is seen waiting.
		hold    func(ctx context.Context, cancel func()) (unblock func())
		wantErr error
	}{
		{"a working holder gives back", func(ctx context.Context, _ func()) func() {
			done := Working(ctx)
			_ = ChargeMem(ctx, 8)
			return func() { ReleaseMem(ctx, 8); done() }
		}, nil},
		{"an earlier turn ends", func(ctx context.Context, _ func()) func() {
			_, end := Turn(ctx)
			_ = ChargeMem(ctx, 1) // past the half a later turn may take
			return end
		}, nil},
		{"the context ends", func(ctx context.Context, cancel func()) func() {
			Working(ctx)
			_ = ChargeMem(ctx, 8)
			return cancel
		}, context.Canceled},
		{"nothing working holds the room", func(ctx context.Context, _ func()) func() {
			_ = ChargeMem(ctx, 8)
			return nil
		}, tooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(WithMemBudget(context.Background(), 10))
			defer cancel()
			unblock := tt.hold(ctx, cancel)
			read, end := Turn(ctx)
			defer end()
			got := make(chan error, 1)
			go func() { got <- ChargeMem(read, 5) }()
			if unblock != nil {
				select {
				case err := <-got:
					t.Fatalf("charged without waiting: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				unblock()
			}
			err := <-got
			var apiErr *APIError
			switch {
			case tt.wantErr == tooLarge:
				if !errors.As(err, &apiErr) || apiErr.Code != ErrCodeTooLarge {
					t.Errorf("err = %v, want too_large", err)
				}
			case !errors.Is(err, tt.wantErr):
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
