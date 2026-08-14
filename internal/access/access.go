package access

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrUnauthenticated  = errors.New("unauthenticated")
	ErrPermissionDenied = errors.New("permission denied")
)

type Operation string

const (
	Append    Operation = "append"
	Read      Operation = "read"
	Subscribe Operation = "subscribe"
	Inspect   Operation = "inspect"
)

type Principal struct {
	Tenant   string
	Service  string
	Instance string
}

func (p Principal) Validate() error {
	if p.Tenant == "" || p.Service == "" || strings.ContainsAny(p.Tenant, "/\x00") || strings.ContainsAny(p.Service, "/\x00") || strings.ContainsAny(p.Instance, "/\x00") {
		return fmt.Errorf("Principal fields are empty or contain reserved characters")
	}
	return nil
}

func (p Principal) Producer() string {
	if p.Instance == "" {
		return p.Tenant + "/" + p.Service
	}
	return p.Tenant + "/" + p.Service + "/" + p.Instance
}

type Authenticator interface {
	Authenticate(context.Context) (Principal, error)
}

type Policy interface {
	Authorize(Principal, string, string, Operation) error
}

type Controller struct {
	Authenticator Authenticator
	Policy        Policy
}

func (c Controller) Authorize(ctx context.Context, namespace, stream string, operation Operation) (Principal, error) {
	if c.Authenticator == nil || c.Policy == nil {
		return Principal{}, fmt.Errorf("%w: access controller is incomplete", ErrUnauthenticated)
	}
	principal, err := c.Authenticator.Authenticate(ctx)
	if err != nil {
		return Principal{}, err
	}
	if err = principal.Validate(); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	if err = c.Policy.Authorize(principal, namespace, stream, operation); err != nil {
		return Principal{}, err
	}
	return principal, nil
}

type Authorizer interface {
	Authorize(context.Context, string, string, Operation) (Principal, error)
}

type AuthorizeFunc func(context.Context, string, string, Operation) (Principal, error)

func (f AuthorizeFunc) Authorize(ctx context.Context, namespace, stream string, operation Operation) (Principal, error) {
	return f(ctx, namespace, stream, operation)
}
