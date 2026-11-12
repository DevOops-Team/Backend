package service

import (
	"fmt"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Kind classifies a service error; the handler maps it to an HTTP status.
type Kind int

const (
	KindInvalid       Kind = iota + 1 // bad input (400)
	KindNotFound                      // no such job, dataset or file (404)
	KindConflict                      // the JobSet already exists (409)
	KindUnprocessable                 // dataset format the model does not read (422)
	KindUpstream                      // S3, the management cluster or DynamoDB failed (502)
	KindTooLarge                      // the upload is over a limit (413)
)

func (k Kind) String() string {
	switch k {
	case KindInvalid:
		return "invalid"
	case KindNotFound:
		return "not found"
	case KindConflict:
		return "conflict"
	case KindUnprocessable:
		return "unprocessable"
	case KindUpstream:
		return "upstream"
	}
	return fmt.Sprintf("Kind(%d)", int(k))
}

// Error is what the service returns for every expected failure. Msg is shown to the user as is.
type Error struct {
	Kind   Kind
	Msg    string
	Code   string             // API error code; empty = the kind's default
	Fields []model.FieldError // per-field reasons for validation errors
	Err    error              // cause, for upstream errors
}

func (e *Error) Error() string { return e.Msg }
func (e *Error) Unwrap() error { return e.Err }

// ErrorCode is the `code` of the API error body.
func (e *Error) ErrorCode() string {
	if e.Code != "" {
		return e.Code
	}
	switch e.Kind {
	case KindInvalid:
		return "VALIDATION"
	case KindNotFound:
		return "NOT_FOUND"
	case KindConflict:
		return "ALREADY_EXISTS"
	case KindUnprocessable:
		return "DATASET_FORMAT_MISMATCH"
	case KindUpstream:
		return "UPSTREAM"
	case KindTooLarge:
		return "TOO_LARGE"
	}
	return ""
}

func invalid(msg string) error { return &Error{Kind: KindInvalid, Msg: msg} }

func invalidFields(fields []model.FieldError) error {
	return &Error{Kind: KindInvalid, Msg: "invalid request: " + fields[0].Field + ": " + fields[0].Message, Fields: fields}
}

func tooLarge(msg string) error { return &Error{Kind: KindTooLarge, Msg: msg} }

func notFound(msg string) error { return &Error{Kind: KindNotFound, Msg: msg} }

// upstream reports a failed dependency call as "<what> failed: <cause>".
func upstream(what string, err error) error {
	return &Error{Kind: KindUpstream, Msg: what + " failed: " + err.Error(), Err: err}
}
