package auth

import "context"

type contextKey string

const ownerIDKey contextKey = "owner_id"

func OwnerIDFromContext(ctx context.Context) (string, bool) {
	v := ctx.Value(ownerIDKey)
	ownerID, ok := v.(string)
	if !ok || ownerID == "" {
		return "", false
	}
	return ownerID, true
}

func withOwnerID(ctx context.Context, ownerID string) context.Context {
	ctx = context.WithValue(ctx, ownerIDKey, ownerID)
	return ctx
}
