package access

import (
	"fmt"
	"strings"
)

type Rule struct {
	Tenant       string      `json:"tenant"`
	Service      string      `json:"service"`
	Instance     string      `json:"instance,omitempty"`
	Namespace    string      `json:"namespace"`
	StreamPrefix string      `json:"stream_prefix"`
	Operations   []Operation `json:"operations"`
}

type StaticPolicy struct {
	Rules []Rule
}

func (p StaticPolicy) Authorize(principal Principal, namespace, stream string, operation Operation) error {
	for _, rule := range p.Rules {
		if rule.Tenant != principal.Tenant || rule.Service != principal.Service || (rule.Instance != "" && rule.Instance != principal.Instance) || rule.Namespace != namespace || !strings.HasPrefix(stream, rule.StreamPrefix) {
			continue
		}
		for _, allowed := range rule.Operations {
			if allowed == operation {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: %s on namespace %q", ErrPermissionDenied, operation, namespace)
}
