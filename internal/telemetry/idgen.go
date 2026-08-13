package telemetry

import (
	"context"
	"crypto/rand"

	"go.opentelemetry.io/otel/trace"
)

type traceIDCtxKey struct{}

// WithTraceID stashes a caller-chosen 128-bit trace ID into ctx. If the next
// span started from this ctx has no parent (i.e. it becomes a trace root),
// requestIDGenerator below uses this value instead of generating a random
// one — this lets a business correlation ID (already used elsewhere to
// match an async request to its response) double as the OTel trace_id, so
// jumping from that ID straight to the Jaeger trace needs no tag search.
func WithTraceID(ctx context.Context, id trace.TraceID) context.Context {
	return context.WithValue(ctx, traceIDCtxKey{}, id)
}

// requestIDGenerator is an sdktrace.IDGenerator. It only special-cases the
// trace_id of a root span, and only when ctx carries one via WithTraceID —
// every other span (children, or roots without a stashed ID) gets a
// cryptographically random ID exactly like the SDK's default generator.
type requestIDGenerator struct{}

func (requestIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	spanID := newRandomSpanID()
	if tid, ok := ctx.Value(traceIDCtxKey{}).(trace.TraceID); ok && tid.IsValid() {
		return tid, spanID
	}
	return newRandomTraceID(), spanID
}

func (requestIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) trace.SpanID {
	return newRandomSpanID()
}

func newRandomTraceID() trace.TraceID {
	var tid trace.TraceID
	_, _ = rand.Read(tid[:])
	return tid
}

func newRandomSpanID() trace.SpanID {
	var sid trace.SpanID
	_, _ = rand.Read(sid[:])
	return sid
}
