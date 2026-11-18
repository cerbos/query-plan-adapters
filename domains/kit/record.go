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
	"sort"
	"strings"
	"time"
)

const pdpRepository = "ghcr.io/cerbos/cerbos"

type pdpPin struct {
	Tag    string `json:"tag"`
	Digest string `json:"digest"`
}

func loadPins(repo string) ([]pdpPin, error) {
	raw, err := os.ReadFile(filepath.Join(repo, "conformance", "pdp-versions.json"))
	if err != nil {
		return nil, err
	}
	var v struct{ Current, Previous pdpPin }
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return []pdpPin{v.Current, v.Previous}, nil
}

type pdp struct {
	name string
	base string
	c    *http.Client
}

func startPDP(ctx context.Context, image, policies string) (*pdp, error) {
	name := fmt.Sprintf("qpa-domains-%d", time.Now().UnixNano())
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--name", name, "-p", "127.0.0.1::3592",
		"-v", policies+":/policies:ro", "-e", "CERBOS_NO_TELEMETRY=1", image,
		"server", "--set=storage.disk.directory=/policies").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w: %s", err, out)
	}
	p := &pdp{name: name, c: &http.Client{Timeout: 60 * time.Second}}
	for i := 0; ; i++ {
		if exec.Command("docker", "exec", name, "/cerbos", "healthcheck").Run() == nil {
			break
		}
		if i == 60 {
			logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
			p.stop()
			return nil, fmt.Errorf("PDP not healthy:\n%s", logs)
		}
		time.Sleep(time.Second)
	}
	port, err := exec.Command("docker", "port", name, "3592/tcp").Output()
	if err != nil {
		p.stop()
		return nil, err
	}
	line := strings.TrimSpace(strings.SplitN(string(port), "\n", 2)[0])
	p.base = "http://127.0.0.1:" + line[strings.LastIndex(line, ":")+1:]
	return p, nil
}

func (p *pdp) stop() { _ = exec.Command("docker", "rm", "-f", p.name).Run() }

func (p *pdp) post(ctx context.Context, path string, body, out any) (string, error) {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.base+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		var e struct{ Message string }
		if json.Unmarshal(b, &e) == nil && e.Message != "" {
			return e.Message, nil
		}
		return "", fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, b)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return "", dec.Decode(out)
}

func principalBody(p map[string]any) map[string]any {
	out := map[string]any{"id": p["id"], "roles": p["roles"]}
	if a, ok := p["attr"]; ok {
		out["attr"] = a
	} else {
		out["attr"] = map[string]any{}
	}
	return out
}

func record(ctx context.Context, repo, dir string) error {
	s, err := loadSchema(dir)
	if err != nil {
		return err
	}
	d, err := loadData(dir, s)
	if err != nil {
		return err
	}
	res, err := project(s, d)
	if err != nil {
		return err
	}
	pins, err := loadPins(repo)
	if err != nil {
		return err
	}
	policies, _ := filepath.Abs(filepath.Join(dir, "policies"))
	domain := filepath.Base(dir)
	if err := os.RemoveAll(filepath.Join(dir, "golden")); err != nil {
		return err
	}
	names := make([]string, 0, len(d.Principals))
	for n := range d.Principals {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, pin := range pins {
		image := fmt.Sprintf("%s:%s@%s", pdpRepository, pin.Tag, pin.Digest)
		p, err := startPDP(ctx, image, policies)
		if err != nil {
			return fmt.Errorf("%s %s: %w", domain, pin.Tag, err)
		}
		err = func() error {
			defer p.stop()
			for _, pn := range names {
				principal := principalBody(d.Principals[pn])
				for _, action := range d.Actions {
					g, err := recordOne(ctx, p, s, res, principal, action)
					if err != nil {
						return fmt.Errorf("%s/%s/%s: %w", pin.Tag, pn, action, err)
					}
					g["domain"], g["pdp"], g["principalName"] = domain, pin.Tag, pn
					path := filepath.Join(dir, "golden", pin.Tag, pn, action+".json")
					if err := writeJSON(path, g); err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func recordOne(ctx context.Context, p *pdp, s *Schema, res []Resource, principal map[string]any, action string) (map[string]any, error) {
	var plan struct {
		Action       string         `json:"action"`
		ResourceKind string         `json:"resourceKind"`
		Filter       map[string]any `json:"filter"`
	}
	msg, err := p.post(ctx, "/api/plan/resources", map[string]any{
		"requestId": "domains", "action": action, "principal": principal,
		"resource": map[string]any{"kind": s.Kind, "attr": map[string]any{}},
	}, &plan)
	if err != nil {
		return nil, err
	}
	g := map[string]any{"principal": principal, "action": action, "resourceKind": s.Kind, "plan": nil, "planError": nil}
	if msg != "" {
		g["planError"] = msg
	} else {
		g["plan"] = plan.Filter
	}
	allowed := []string{}
	for start := 0; start < len(res); start += 50 {
		end := min(start+50, len(res))
		var entries []any
		for _, r := range res[start:end] {
			entries = append(entries, map[string]any{"actions": []string{action},
				"resource": map[string]any{"kind": s.Kind, "id": r.ID, "attr": r.Attr}})
		}
		var resp struct {
			Results []struct {
				Resource struct{ ID string } `json:"resource"`
				Actions  map[string]string   `json:"actions"`
			} `json:"results"`
		}
		msg, err := p.post(ctx, "/api/check/resources", map[string]any{"requestId": "domains", "principal": principal, "resources": entries}, &resp)
		if err != nil {
			return nil, err
		}
		if msg != "" {
			return nil, fmt.Errorf("check: %s", msg)
		}
		for i, r := range resp.Results {
			if r.Resource.ID != res[start+i].ID {
				return nil, fmt.Errorf("check result order")
			}
			if r.Actions[action] == "EFFECT_ALLOW" {
				allowed = append(allowed, r.Resource.ID)
			}
		}
	}
	g["allowed"] = allowed
	switch len(allowed) {
	case 0:
		g["oracle"] = "empty"
	case len(res):
		g["oracle"] = "total"
	default:
		g["oracle"] = "discriminating"
	}
	return g, nil
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
