package ratelimiting

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/anondeveg/jam/internal/database"
)

type FixedWindow struct {
	limit  int
	window time.Duration
	redis  database.Redis
}

func NewFixedWindow(limit int, window time.Duration, redis database.Redis) *FixedWindow {

	redis.NewConnection()
	return &FixedWindow{limit: limit, window: window, redis: redis}

}

func getClientIp(req *http.Request) (string, error) {
	if req == nil || req.Header == nil {
		return "", nil
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)

	if err != nil {
		return "", err
	}
	return host, nil
}

func (t *FixedWindow) islimited(req *http.Request, key string) (bool, time.Duration, error) {
	pipe := t.redis.Connection.TxPipeline()
	count := pipe.Incr(req.Context(), key)
	pipe.ExpireNX(req.Context(), key, t.window)
	ttl := pipe.PTTL(req.Context(), key)

	_, err := pipe.Exec(req.Context())
	if err != nil {
		return false, 0, err
	}
	if count.Val() > int64(t.limit) {
		return true, ttl.Val(), nil
	}
	return false, 0, nil

}
func (t *FixedWindow) RateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//before
		ip, err := getClientIp(r)
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		islimited, duration, err := t.islimited(r, "ratelimitFW:"+ip)
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if islimited {
			secs := int(math.Max(math.Ceil(duration.Seconds()), 1))
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
