package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// maxResourcesPerCheck stays under the PDP's default server.requestLimits.maxResourcesPerRequest (50).
const maxResourcesPerCheck = 50

// pdp is one running Cerbos container.
type pdp struct {
	container string
	base      string
	client    *http.Client
}

var unsafeName = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// startPDP starts image with policiesDir mounted read-only and waits for it to be healthy.
func startPDP(ctx context.Context, image, tag, policiesDir string) (*pdp, error) {
	name := fmt.Sprintf("cerbos-conformance-generator-%d-%s", os.Getpid(), unsafeName.ReplaceAllString(tag, "_"))
	_ = exec.Command("docker", "rm", "-f", name).Run()

	out, err := exec.CommandContext(ctx, "docker", "run", "-d",
		"--name", name,
		"-p", "127.0.0.1::3592",
		"-v", policiesDir+":/policies:ro",
		"-e", "CERBOS_NO_TELEMETRY=1",
		image,
		"server", "--set=storage.disk.directory=/policies",
	).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker run %s: %w: %s", image, err, out)
	}
	p := &pdp{container: name, client: &http.Client{Timeout: 60 * time.Second}}

	healthy := false
	for i := 0; i < 60; i++ {
		if exec.CommandContext(ctx, "docker", "exec", name, "/cerbos", "healthcheck").Run() == nil {
			healthy = true
			break
		}
		select {
		case <-ctx.Done():
			p.stop()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if !healthy {
		logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
		p.stop()
		return nil, fmt.Errorf("Cerbos PDP %s did not become healthy within 60 seconds:\n%s", tag, logs)
	}

	portOut, err := exec.CommandContext(ctx, "docker", "port", name, "3592/tcp").Output()
	if err != nil {
		p.stop()
		return nil, fmt.Errorf("docker port: %w", err)
	}
	line := strings.TrimSpace(strings.SplitN(string(portOut), "\n", 2)[0])
	port := line[strings.LastIndex(line, ":")+1:]
	p.base = "http://127.0.0.1:" + port
	return p, nil
}

func (p *pdp) stop() {
	_ = exec.Command("docker", "rm", "-f", p.container).Run()
}

// errorResponse is the body the PDP returns for a failed request.
type errorResponse struct {
	Message string `json:"message"`
}

// post returns the decoded body on 200, or the PDP's error message on any other status.
func (p *pdp) post(ctx context.Context, path string, body any, out any) (pdpErr string, err error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+path, bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		var e errorResponse
		if json.Unmarshal(respBody, &e) == nil && e.Message != "" {
			return e.Message, nil
		}
		return "", fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, respBody)
	}
	dec := json.NewDecoder(bytes.NewReader(respBody))
	dec.UseNumber()
	return "", dec.Decode(out)
}

// planResult is one PlanResources outcome.
type planResult struct {
	Filter map[string]any // nil when planning failed
	Error  string
}

func (p *pdp) plan(ctx context.Context, principal map[string]any, action string) (planResult, error) {
	body := map[string]any{
		"requestId": "conformance-generator",
		"action":    action,
		"principal": principal,
		"resource":  map[string]any{"kind": resourceKind, "attr": map[string]any{}},
	}
	var resp struct {
		Action       string         `json:"action"`
		ResourceKind string         `json:"resourceKind"`
		Filter       map[string]any `json:"filter"`
	}
	before := time.Now().UTC()
	msg, err := p.post(ctx, "/api/plan/resources", body, &resp)
	after := time.Now().UTC()
	if err != nil {
		return planResult{}, err
	}
	if msg != "" {
		return planResult{Error: msg}, nil
	}
	if resp.Action != action || resp.ResourceKind != resourceKind {
		return planResult{}, fmt.Errorf("plan %q: response names action %q, kind %q", action, resp.Action, resp.ResourceKind)
	}
	switch resp.Filter["kind"] {
	case "KIND_ALWAYS_ALLOWED", "KIND_ALWAYS_DENIED", "KIND_CONDITIONAL":
	default:
		return planResult{}, fmt.Errorf("plan %q: no recognised filter kind in %v", action, resp.Filter)
	}
	if err := replaceNow(resp.Filter, before, after); err != nil {
		return planResult{}, fmt.Errorf("plan %q: %w", action, err)
	}
	return planResult{Filter: resp.Filter}, nil
}

// check returns the ids of the resources the PDP allows action on, in dataset order.
func (p *pdp) check(ctx context.Context, ds *Dataset, action string) ([]string, error) {
	var allowed []string
	for start := 0; start < len(ds.Resources); start += maxResourcesPerCheck {
		end := min(start+maxResourcesPerCheck, len(ds.Resources))
		entries := make([]any, 0, end-start)
		for _, r := range ds.Resources[start:end] {
			entries = append(entries, map[string]any{
				"actions":  []string{action},
				"resource": map[string]any{"kind": resourceKind, "id": r.ID, "attr": r.Attr},
			})
		}
		body := map[string]any{
			"requestId": "conformance-generator",
			"principal": ds.Principal,
			"resources": entries,
		}
		var resp struct {
			Results []struct {
				Resource struct {
					ID string `json:"id"`
				} `json:"resource"`
				Actions map[string]string `json:"actions"`
			} `json:"results"`
		}
		msg, err := p.post(ctx, "/api/check/resources", body, &resp)
		if err != nil {
			return nil, err
		}
		if msg != "" {
			return nil, fmt.Errorf("check %q: %s", action, msg)
		}
		if len(resp.Results) != end-start {
			return nil, fmt.Errorf("check %q: %d results for %d resources", action, len(resp.Results), end-start)
		}
		for i, res := range resp.Results {
			want := ds.Resources[start+i].ID
			if res.Resource.ID != want {
				return nil, fmt.Errorf("check %q: result %d is for %q, want %q", action, start+i, res.Resource.ID, want)
			}
			switch res.Actions[action] {
			case "EFFECT_ALLOW":
				allowed = append(allowed, want)
			case "EFFECT_DENY":
			default:
				return nil, fmt.Errorf("check %q: resource %q has effect %q", action, want, res.Actions[action])
			}
		}
	}
	return allowed, nil
}

// nowPlaceholder replaces the planner's folded `now() - duration("24h")` so replans do not
// churn.
const nowPlaceholder = "__NOW_MINUS_24H__"

// clockSlack bounds how far a folded now() may sit from the request's own clock readings.
const clockSlack = 2 * time.Minute

// replaceNow walks a plan and replaces every timestamp literal the planner folded from
// now() - 24h with nowPlaceholder. A literal near now() itself (or any other offset the corpus
// has no placeholder for) is an error: it would make the golden file differ on every run.
func replaceNow(node any, before, after time.Time) error {
	switch n := node.(type) {
	case map[string]any:
		if expr, ok := n["expression"].(map[string]any); ok && expr["operator"] == "timestamp" {
			if ops, ok := expr["operands"].([]any); ok && len(ops) > 0 {
				if op, ok := ops[0].(map[string]any); ok {
					if s, ok := op["value"].(string); ok {
						if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
							switch {
							case within(t, before.Add(-24*time.Hour), after.Add(-24*time.Hour)):
								op["value"] = nowPlaceholder
							case within(t, before.Add(-48*time.Hour), after.Add(24*time.Hour)):
								return fmt.Errorf("timestamp literal %s is close to the planning time but is not now() - 24h; it would change on every run", s)
							}
						}
					}
				}
			}
		}
		for _, v := range n {
			if err := replaceNow(v, before, after); err != nil {
				return err
			}
		}
	case []any:
		for _, v := range n {
			if err := replaceNow(v, before, after); err != nil {
				return err
			}
		}
	}
	return nil
}

func within(t, lo, hi time.Time) bool {
	return !t.Before(lo.Add(-clockSlack)) && !t.After(hi.Add(clockSlack))
}

// stagePolicies copies the generated policy and the hand-written derived roles into a fresh
// directory for the PDP to mount. Mounting only these two keeps the generator independent of
// whatever else sits in conformance/policies/.
func stagePolicies(policy []byte, derivedRolesPath string) (string, error) {
	dir, err := os.MkdirTemp("", "conformance-generator-policies-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return "", err
	}
	derived, err := os.ReadFile(derivedRolesPath)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "conformance.yaml"), policy, 0o644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "derived_roles.yaml"), derived, 0o644); err != nil {
		return "", err
	}
	return dir, nil
}
