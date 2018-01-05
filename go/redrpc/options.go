package redrpc

import "time"

const (
	DefaultPrefix = "redis_rpc"

	DefaultBLPOPTimeout    = 1 * time.Second
	DefaultResponseTimeout = 1 * time.Second

	DefaultRequestExpire = 120 * time.Second
	DefaultResultExpire  = 120 * time.Second
)

type Options struct {
	Prefix                                                     string
	RequestExpire, ResultExpire, ResponseTimeout, BLPOPTimeout time.Duration
	TimeSource                                                 func() time.Time
}

func OptsWithDefaults(o *Options) *Options {
	var ret Options
	if o != nil {
		ret = *o
	}

	if ret.Prefix == "" {
		ret.Prefix = DefaultPrefix
	}
	if ret.RequestExpire == 0 {
		ret.RequestExpire = DefaultRequestExpire
	}
	if ret.ResultExpire == 0 {
		ret.ResultExpire = DefaultResultExpire
	}
	if ret.ResponseTimeout == 0 {
		ret.ResponseTimeout = DefaultResponseTimeout
	}
	if ret.BLPOPTimeout == 0 {
		ret.BLPOPTimeout = DefaultBLPOPTimeout
	}
	if ret.TimeSource == nil {
		ret.TimeSource = time.Now
	}
	return &ret
}
