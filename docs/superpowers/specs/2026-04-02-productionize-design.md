# Queuer Productionization Design

## Overview

Make the queuer library production-ready: versioning, CI/CD, robustness fixes, API polish, and linting.

## 1. Versioning & Releases

- Tag current state as `v0.1.0` (first unstable release).
- Add `CHANGELOG.md` following Keep a Changelog format.
- Use semver: `v0.x.y` until API stabilizes, then `v1.0.0`.
- GitHub Actions release workflow: tag push (`v*`) triggers release with changelog entry.

## 2. CI/CD — GitHub Actions

### On push/PR to main (`.github/workflows/ci.yml`)

- `go build ./...`
- `go vet ./...`
- `golangci-lint run`
- `go test ./... -race -timeout 60s`
- Test matrix: Go 1.22 and latest

### On tag push (`.github/workflows/release.yml`)

- Run all checks above
- Create GitHub Release with changelog entry extracted from `CHANGELOG.md`

## 3. Robustness

### Panic recovery in workers

If a handler panics, recover it inside the worker goroutine. Log the panic with stack trace via `slog`. Treat it as a handler error routed through the `ErrorResolver` with a wrapped `PanicError`. Without this, one bad message permanently kills a worker goroutine, silently reducing pool capacity.

### Receive error backoff

Currently, if `Receive` fails (e.g., SQS throttling), the poll loop retries immediately in a hot loop. Add exponential backoff:

- Start at 1s
- Double each consecutive failure: 1s → 2s → 4s → 8s → 16s → 30s (capped)
- Reset to 0 on successful receive

Implementation: simple counter in the poll loop, `time.Sleep(backoff)` before retry. No jitter needed for a single consumer.

### Context propagation on shutdown

Current problem: `processMessage` receives the parent `ctx`, which is already cancelled during shutdown. Handlers that respect `ctx.Err()` will bail out on in-flight messages instead of finishing their work.

Fix: create a separate processing context derived from `context.Background()` that is only cancelled when the shutdown timeout fires. The parent `ctx` controls the poll loop; the processing context controls in-flight work.

```
parentCtx cancelled → poll loop stops
                    → shutdown timer starts
                    → processingCtx cancelled only if timer fires
```

### Input validation

`New()` panics if `receiver`, `handler`, or `acknowledger` is nil. Fail fast at construction time, not at runtime deep in a goroutine where the nil pointer panic is harder to diagnose.

## 4. API Polish

### Sentinel errors

Export typed errors for programmatic checking:

```go
var ErrShutdownTimeout = errors.New("queuer: shutdown timeout exceeded")
```

Callers can use `errors.Is(err, queuer.ErrShutdownTimeout)`.

### PanicError

New error type wrapping recovered panics:

```go
type PanicError struct {
    Value any
    Stack []byte
}
```

Returned by the panic recovery in workers, routed through the ErrorResolver so callers can handle panics differently from normal errors.

### Godoc examples

Add `example_test.go` with runnable examples:

- `ExampleHandlerFunc` — basic consumer with function handler
- `ExampleErrorResolver` — custom error resolver with per-error actions
- `ExampleConsumer_full` — full setup with metrics, tracing, and custom error resolver

### Existing API (no changes needed)

- `ErrorAction.String()` — already exists
- `HandlerFunc` adapter — already exists

## 5. Linting & Code Quality

### `.golangci-lint.yml`

```yaml
linters:
  enable:
    - govet
    - staticcheck
    - errcheck
    - ineffassign
    - unused
    - gosimple
    - gofmt
    - goimports

linters-settings:
  errcheck:
    check-type-assertions: true

issues:
  exclude-use-default: false
```

No overly aggressive linters (`wsl`, `godox`, etc.).

## Files Changed

| File | Change |
|------|--------|
| `consumer.go` | Panic recovery, receive backoff, processing context, input validation |
| `errors.go` | Add `ErrShutdownTimeout`, `PanicError` |
| `example_test.go` | New: godoc examples |
| `CHANGELOG.md` | New: changelog |
| `.github/workflows/ci.yml` | New: CI pipeline |
| `.github/workflows/release.yml` | New: release pipeline |
| `.golangci-lint.yml` | New: linter config |
| `CLAUDE.md` | Update with new commands |
| `README.md` | Update with version badge |

## Testing

- Existing tests continue to pass unchanged.
- New tests:
  - `TestConsumer_PanicRecovery` — handler panics, worker survives, error resolver receives `PanicError`
  - `TestConsumer_ReceiveBackoff` — verify delay between retries on receive error
  - `TestConsumer_ShutdownContextPropagation` — verify in-flight handlers get a non-cancelled context during shutdown
  - `TestNew_NilReceiver_Panics` — verify nil argument validation
